// The key-value server implementation

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>

#include "defs.h"
#include "hash.h"
#include "util.h"


// Program arguments

// Host name and port number of the metadata server
static char mserver_host_name[HOST_NAME_MAX] = "";
static uint16_t mserver_port = 0;

// Ports for listening to incoming connections from clients, servers and mserver
static uint16_t clients_port = 0;
static uint16_t servers_port = 0;
static uint16_t mservers_port = 0;

// Current server id and total number of servers
static int server_id = -1;
static int num_servers = 0;

// Log file name
static char log_file_name[PATH_MAX] = "";


static void usage(char **argv)
{
	printf("usage: %s -h <mserver host> -m <mserver port> -c <clients port> -s <servers port> "
	       "-M <mservers port> -S <server id> -n <num servers> [-l <log file>]\n", argv[0]);
	printf("If the log file (-l) is not specified, log output is written to stdout\n");
}

// Returns false if the arguments are invalid
static bool parse_args(int argc, char **argv)
{
	char option;
	while ((option = getopt(argc, argv, "h:m:c:s:M:S:n:l:")) != -1) {
		switch(option) {
			case 'h': strncpy(mserver_host_name, optarg, HOST_NAME_MAX); break;
			case 'm': mserver_port  = atoi(optarg); break;
			case 'c': clients_port  = atoi(optarg); break;
			case 's': servers_port  = atoi(optarg); break;
			case 'M': mservers_port = atoi(optarg); break;
			case 'S': server_id     = atoi(optarg); break;
			case 'n': num_servers   = atoi(optarg); break;
			case 'l': strncpy(log_file_name, optarg, PATH_MAX); break;
			default:
				fprintf(stderr, "Invalid option: -%c\n", option);
				return false;
		}
	}

	return (mserver_host_name[0] != '\0') && (mserver_port != 0) && (clients_port != 0) && (servers_port != 0) &&
	       (mservers_port != 0) && (num_servers >= 3) && (server_id >= 0) && (server_id < num_servers);
}


// Socket for sending requests to the metadata server
static int mserver_fd_out = -1;
// Socket for receiving requests from the metadata server
static int mserver_fd_in = -1;

// Sockets for listening for incoming connections from clients, servers and mserver
static int my_clients_fd = -1;
static int my_servers_fd = -1;
static int my_mservers_fd = -1;

// Store fds for all connected clients, up to MAX_CLIENT_SESSIONS
#define MAX_CLIENT_SESSIONS 1000
static int client_fd_table[MAX_CLIENT_SESSIONS];

// Store fds for connected servers
static int server_fd_table[2] = {-1, -1};


// Storage for primary key set
hash_table primary_hash = {0};

// Storage for secondary key set
hash_table secondary_hash = {0};

// Primary server (the one that stores the primary copy for this server's secondary key set)
static int primary_sid = -1;
static int primary_fd = -1;

// Secondary server (the one that stores the secondary copy for this server's primary key set)
static int secondary_sid = -1;
static int secondary_fd = -1;


static pthread_t client_thread;

// Period heartbeat messages
static const int heartbeat_interval = 1;  // in seconds
static pthread_t heartbeat_thread;

// For recovery flow
static kv_server_state state;
static bool send_primary;
static pthread_t send_replacement_primary_thread;
static pthread_t send_replacement_secondary_thread;


static void cleanup();

static const int hash_size = 65536;


// Sends periodic heartbeat messages to metadata server
static void *heartbeat_task(void *args)
{
	for (;;) {
		mserver_ctrl_request request = {0};
		request.hdr.type = MSG_MSERVER_CTRL_REQ;
		request.type = HEARTBEAT;
		request.server_id = server_id;
		send_msg(mserver_fd_out, &request, sizeof(request));

		sleep(heartbeat_interval);
	}

	return NULL;
}

static void send_table_iterator_f(const char key[KEY_SIZE], void *value, size_t value_sz, void *arg)
{
	(void)arg;

	// Package key/value into request packet
	char buffer[MAX_MSG_LEN] = {0};
	operation_request *request = (operation_request *)buffer;
	request->hdr.type = MSG_OPERATION_REQ;
	request->type = OP_PUT;
	memcpy(request->key, key, KEY_SIZE);
	strncpy(request->value, value, value_sz);

	// Send PUT request to new server (Saa)
	int new_fd = send_primary ? secondary_fd : primary_fd;
	send_msg(new_fd, request, sizeof(*request) + value_sz);
}

// Sends a set to a replacement server for recovery
static void *send_table_task(void *arg)
{
	(void)arg;

	hash_table *table = send_primary ? &primary_hash : &secondary_hash;
	hash_iterate(table, send_table_iterator_f, NULL);

	// 8/10. Send confirmation to M server when done sending the set
	mserver_ctrl_request request = {0};
	request.hdr.type = MSG_MSERVER_CTRL_REQ;
	request.server_id = server_id;
	request.type = send_primary ? UPDATED_SECONDARY : UPDATED_PRIMARY;
	send_msg(mserver_fd_out, &request, sizeof(request));

	state = KV_SERVER_ONLINE;

	return NULL;
}

// Sends secondary set to a replacement key-value server as part of the recovery flow
static int send_to_replacement(const char *host_name, uint16_t port)
{
	pthread_t *replacement_thread = NULL;

	int new_fd;
	if ((new_fd = connect_to_server(host_name, port)) < 0) {
		fprintf(stderr, "send_to_replacement: error connecting to new secondary_fd\n");
		goto send_replacement_failed;
	}

	// Connect to the new recovery server
	if (send_primary) {
		// Sc: connect to new Saa as secondary
		close_safe(&secondary_fd);
		secondary_fd = new_fd;

		// [UPDATE_SECONDARY] Sending primary: this primary is the recovering server's secondary set
		state = KV_UPDATING_SECONDARY;
		replacement_thread = &send_replacement_secondary_thread;
	} else {
		close_safe(&primary_fd);
		primary_fd = new_fd;

		// [UPDATE_PRIMARY] Sending secondary: this secondary is the recovering server's primary set
		state = KV_UPDATING_PRIMARY;
		replacement_thread = &send_replacement_primary_thread;
	}

	// Spawn a new thread to asynchronously send the set to the replacement server
	if (pthread_create(replacement_thread, NULL, send_table_task, NULL)) {
		fprintf(stderr, "send_to_replacement: error creating thread\n");
		goto send_replacement_failed;
	}

	return 0;

send_replacement_failed:
	// Rollback
	state = KV_SERVER_ONLINE;

	// Send FAILED message to M server
	mserver_ctrl_request request = {0};
	request.hdr.type = MSG_MSERVER_CTRL_REQ;
	request.server_id = server_id;
	request.type = send_primary ? UPDATE_SECONDARY_FAILED : UPDATE_PRIMARY_FAILED;
	send_msg(mserver_fd_out, &request, sizeof(request));

	return -1;
}

// Initialize and start the server
static bool init_server()
{
	for (int i = 0; i < MAX_CLIENT_SESSIONS; i++) {
		client_fd_table[i] = -1;
	}

	// Get the host name that server is running on
	char my_host_name[HOST_NAME_MAX] = "";
	if (get_local_host_name(my_host_name, sizeof(my_host_name)) < 0) {
		return false;
	}
	log_write("%s Server starts on host: %s\n", current_time_str(), my_host_name);

	// Create sockets for incoming connections from clients and other servers
	if (((my_clients_fd  = create_server(clients_port, MAX_CLIENT_SESSIONS, NULL)) < 0) ||
	    ((my_servers_fd  = create_server(servers_port, 2, NULL)) < 0) ||
	    ((my_mservers_fd = create_server(mservers_port, 1, NULL)) < 0))
	{
		goto cleanup;
	}

	// Connect to mserver to "register" that we are live
	if ((mserver_fd_out = connect_to_server(mserver_host_name, mserver_port)) < 0) {
		goto cleanup;
	}

	// Determine the ids of replica servers
	primary_sid = primary_server_id(server_id, num_servers);
	secondary_sid = secondary_server_id(server_id, num_servers);

	// Initialize key-value storage
	if (!hash_init(&primary_hash, hash_size)) {
		goto cleanup;
	}
	if (!hash_init(&secondary_hash, hash_size)) {
		goto cleanup;
	}

	state = KV_SERVER_ONLINE;

	// Create a separate thread that takes care of sending periodic heartbeat messages
	if (pthread_create(&heartbeat_thread, NULL, heartbeat_task, NULL)) {
		perror("init_server: heartbeat thread create\n");
		goto cleanup;
	}

	log_write("Server initialized\n");
	return true;

cleanup:
	cleanup();
	return false;
}

// Hash iterator for freeing memory used by values; called during storage cleanup
static void clean_iterator_f(const char key[KEY_SIZE], void *value, size_t value_sz, void *arg)
{
	(void)key;
	(void)value_sz;
	(void)arg;

	assert(value != NULL);
	free(value);
}

// Cleanup and release all the resources
static void cleanup()
{
	close_safe(&mserver_fd_out);
	close_safe(&mserver_fd_in);
	close_safe(&my_clients_fd);
	close_safe(&my_servers_fd);
	close_safe(&my_mservers_fd);
	close_safe(&secondary_fd);
	close_safe(&primary_fd);

	for (int i = 0; i < MAX_CLIENT_SESSIONS; i++) {
		close_safe(&(client_fd_table[i]));
	}
	for (int i = 0; i < 2; i++) {
		close_safe(&(server_fd_table[i]));
	}

	hash_iterate(&primary_hash, clean_iterator_f, NULL);
	hash_cleanup(&primary_hash);

	hash_iterate(&secondary_hash, clean_iterator_f, NULL);
	hash_cleanup(&secondary_hash);

	// Cancel threads
	if (client_thread) {
		pthread_cancel(client_thread);
	}
	if (heartbeat_thread) {
		pthread_cancel(heartbeat_thread);
	}
	if (send_replacement_primary_thread) {
		pthread_cancel(send_replacement_primary_thread);
	}
	if (send_replacement_secondary_thread) {
		pthread_cancel(send_replacement_secondary_thread);
	}
}

// Connection will be closed after calling this function regardless of result
static void process_client_message(int fd)
{
	// log_write("%s Receiving a client message\n", current_time_str());

	// Read and parse the message
	char req_buffer[MAX_MSG_LEN] = {0};
	if (!recv_msg(fd, req_buffer, MAX_MSG_LEN, MSG_OPERATION_REQ)) {
		return;
	}
	operation_request *request = (operation_request*)req_buffer;

	// Initialize the response
	char resp_buffer[MAX_MSG_LEN] = {0};
	operation_response *response = (operation_response*)resp_buffer;
	response->hdr.type = MSG_OPERATION_RESP;
	uint16_t value_sz = 0;

	// Check that requested key is valid if this is supposed to be the primary server
	int key_srv_id = key_server_id(request->key, num_servers);
	int secondary_srv_id = secondary_server_id(key_srv_id, num_servers);

	// When normal or updating secondary (Sc), we're targetting the primary set
	// If this is Sb, then we can target either set
	if ((state != KV_UPDATING_PRIMARY && key_srv_id != server_id) ||
	    (state == KV_UPDATING_PRIMARY && key_srv_id != server_id && secondary_srv_id != server_id)) {
		fprintf(stderr, "sid %d: Invalid client key %s sid %d\n", server_id, key_to_str(request->key), key_srv_id);
		response->status = SERVER_FAILURE;
		send_msg(fd, response, sizeof(*response));
		return;
	}

	// Targetting secondary set as a pseudo-primary set
	bool secondary_as_primary = (state == KV_UPDATING_PRIMARY && secondary_srv_id == server_id);

	hash_table *table = secondary_as_primary ? &secondary_hash : &primary_hash;

	// Process the request based on its type
	switch (request->type) {
		case OP_NOOP: {
			response->status = SUCCESS;
			break;
		}

		case OP_GET: {
			void *data = NULL;
			size_t size = 0;

			// Get the value for requested key from the hash table
			if (!hash_get(table, request->key, &data, &size)) {
				fprintf(stderr, "Key %s not found\n", key_to_str(request->key));
				response->status = KEY_NOT_FOUND;
				break;
			}

			// Copy the stored value into the response buffer
			memcpy(response->value, data, size);
			value_sz = size;

			response->status = SUCCESS;
			break;
		}

		case OP_PUT: {
			// Need to copy the value to dynamically allocated memory
			size_t value_size = request->hdr.length - sizeof(*request);
			void *value_copy = malloc(value_size);
			if (value_copy == NULL) {
				perror("malloc");
				fprintf(stderr, "sid %d: Out of memory\n", server_id);
				response->status = OUT_OF_SPACE;
				break;
			}
			memcpy(value_copy, request->value, value_size);

			void *old_value = NULL;
			size_t old_value_sz = 0;

			hash_lock(table, request->key);

			// Put the <key, value> pair into the hash table
			if (!hash_put(table, request->key, value_copy, value_size, &old_value, &old_value_sz))
			{
				hash_unlock(table, request->key);
				fprintf(stderr, "sid %d: Out of memory\n", server_id);
				free(value_copy);
				response->status = OUT_OF_SPACE;
				break;
			}

			// Forward the PUT request to the secondary replica
			// 7. If in recovery mode, PUT requests are sent synchronously to the new server too
			int forward_fd = secondary_as_primary ? primary_fd : secondary_fd;
			if (fd_is_valid(forward_fd)) {
				send_msg(forward_fd, request, request->hdr.length);

				char forward_resp_buffer[MAX_MSG_LEN] = {0};
				operation_response *forward_server_resp = (operation_response *)forward_resp_buffer;
				if (!recv_msg(forward_fd, forward_server_resp, sizeof(*forward_server_resp), MSG_OPERATION_RESP)) {
					hash_unlock(table, request->key);
					return;
				}

				if (forward_server_resp->status != SUCCESS) {
					fprintf(stderr, "Server %d failed PUT forwarding (%s)\n", server_id, op_status_str[forward_server_resp->status]);
					hash_unlock(table, request->key);
					return;
				}
			}

			hash_unlock(table, request->key);

			// Need to free the old value (if there was any)
			if (old_value != NULL) {
				free(old_value);
			}

			response->status = SUCCESS;
			break;
		}

		default: {
			fprintf(stderr, "sid %d: Invalid client operation type\n", server_id);
			return;
		}
	}

	// Send reply to the client
	send_msg(fd, response, sizeof(*response) + value_sz);
}

// Returns false if either the message was invalid or if this was the last message
// (in both cases the connection will be closed)
static bool process_server_message(int fd)
{
	// log_write("%s Receiving a server message\n", current_time_str());

	// Read and parse the message
	char req_buffer[MAX_MSG_LEN] = {0};
	if (!recv_msg(fd, req_buffer, MAX_MSG_LEN, MSG_OPERATION_REQ)) {
		return false;
	}
	operation_request *request = (operation_request*)req_buffer;

	// Initialize the response
	char resp_buffer[MAX_MSG_LEN] = {0};
	operation_response *response = (operation_response*)resp_buffer;
	response->hdr.type = MSG_OPERATION_RESP;
	uint16_t value_sz = 0;

	switch (request->type) {
		// NOOP operation request is used to indicate the last message in an UPDATE sequence
		case OP_NOOP: {
			// log_write("Received the last server message, closing connection\n");
			return false;
		}

		case OP_PUT: {
			// Need to copy the value to dynamically allocated memory
			size_t value_size = request->hdr.length - sizeof(*request);
			void *value_copy = malloc(value_size);
			if (value_copy == NULL) {
				perror("malloc");
				fprintf(stderr, "sid %d: Out of memory\n", server_id);
				response->status = OUT_OF_SPACE;
				break;
			}
			memcpy(value_copy, request->value, value_size);

			void *old_value = NULL;
			size_t old_value_sz = 0;

			int primary_srv_id = key_server_id(request->key, num_servers);
			int secondary_srv_id = secondary_server_id(primary_srv_id, num_servers);

			// Somehow this server isn't the primary nor secondary server for the given key
			if (server_id != primary_srv_id && server_id != secondary_srv_id) {
				fprintf(stderr, "sid %d: Received server message but this server does not handle the key\n", server_id);
				response->status = SERVER_FAILURE;
				break;
			}

			// Normally, this is for putting it in the secondary replica (forwarded PUT)
			// During recovery, we might need to update the new primary replica instead (Saa)
			hash_table *table = (server_id == primary_srv_id) ? &primary_hash : &secondary_hash;

			hash_lock(table, request->key);

			// Put the <key, value> pair into the hash table
			if (!hash_put(table, request->key, value_copy, value_size, &old_value, &old_value_sz))
			{
				hash_unlock(table, request->key);
				fprintf(stderr, "sid %d: Out of memory\n", server_id);
				free(value_copy);
				response->status = OUT_OF_SPACE;
				break;
			}

			hash_unlock(table, request->key);

			// Need to free the old value (if there was any)
			if (old_value != NULL) {
				free(old_value);
			}

			response->status = SUCCESS;
			break;
		}

		default: {
			fprintf(stderr, "sid %d: Invalid server operation type\n", server_id);
			response->status = SERVER_FAILURE;
			break;
		}
	}

	send_msg(fd, response, sizeof(*response) + value_sz);
	return true;
}

// Returns false if the message was invalid (so the connection will be closed)
// Sets *shutdown_requested to true if received a SHUTDOWN message (so the server will terminate)
static bool process_mserver_message(int fd, bool *shutdown_requested)
{
	assert(shutdown_requested != NULL);
	*shutdown_requested = false;

	// log_write("%s Receiving a metadata server message\n", current_time_str());

	// Read and parse the message
	char req_buffer[MAX_MSG_LEN] = {0};
	if (!recv_msg(fd, req_buffer, MAX_MSG_LEN, MSG_SERVER_CTRL_REQ)) {
		return false;
	}
	server_ctrl_request *request = (server_ctrl_request*)req_buffer;

	// Initialize the response
	server_ctrl_response response = {0};
	response.hdr.type = MSG_SERVER_CTRL_RESP;

	// Process the request based on its type
	switch (request->type) {
		case SET_SECONDARY: {
			response.status = ((secondary_fd = connect_to_server(request->host_name, request->port)) < 0)
			                ? CTRLREQ_FAILURE : CTRLREQ_SUCCESS;
			break;
		}

		case SHUTDOWN: {
			*shutdown_requested = true;
			return true;
		}

		case UPDATE_PRIMARY: {
			send_primary = false;
			response.status = (send_to_replacement(request->host_name, request->port) < 0)
			                ? CTRLREQ_FAILURE : CTRLREQ_SUCCESS;
			break;
		}

		case UPDATE_SECONDARY: {
			send_primary = true;
			response.status = (send_to_replacement(request->host_name, request->port) < 0)
			                ? CTRLREQ_FAILURE : CTRLREQ_SUCCESS;
			break;
		}

		// Only Sb should get this
		case SWITCH_PRIMARY: {
			state = KV_SWITCHING_PRIMARY;

			// 14. Flush all remaining updates to new server
			for (int i = 0; i < MAX_CLIENT_SESSIONS; i++) {
				if (fd_is_valid(client_fd_table[i])) {
					process_client_message(client_fd_table[i]);
					close_safe(&(client_fd_table[i]));
				}
			}

			// 15. Do the switch and send a confirmation message
			response.status = CTRLREQ_SUCCESS;

			state = KV_SERVER_ONLINE;

			break;
		}

		default: {
			// Impossible
			assert(false);
			break;
		}
	}

	send_msg(fd, &response, sizeof(response));
	return true;
}

static void *process_client_task(void *args)
{
	// Usual preparation stuff for select()
	fd_set rset, allset;
	FD_ZERO(&allset);
	FD_SET(my_clients_fd, &allset);;

	int maxfd = my_clients_fd;

	for (;;) {
		rset = allset;

		int num_ready_fds = select(maxfd + 1, &rset, NULL, NULL, NULL);
		if (num_ready_fds < 0) {
			perror("select");
			return false;
		}

		if (num_ready_fds <= 0) {
			continue;
		}

		// Incoming connection from a client
		if (FD_ISSET(my_clients_fd, &rset)) {
			int fd_idx = accept_connection(my_clients_fd, client_fd_table, MAX_CLIENT_SESSIONS);
			if (fd_idx >= 0) {
				FD_SET(client_fd_table[fd_idx], &allset);
				maxfd = max(maxfd, client_fd_table[fd_idx]);
			}

			if (--num_ready_fds <= 0) {
				continue;
			}
		}

		// Check for any messages from connected clients
		for (int i = 0; i < MAX_CLIENT_SESSIONS; i++) {
			if ((client_fd_table[i] != -1) && FD_ISSET(client_fd_table[i], &rset)) {
				// Explicitely ignore client requests while handling SWITCH_PRIMARY
				if (state == KV_SWITCHING_PRIMARY) {
					operation_response response = {0};
					response.hdr.type = MSG_OPERATION_RESP;
					response.status = SERVER_FAILURE;
					send_msg(client_fd_table[i], &response, sizeof(response));
				} else {
					process_client_message(client_fd_table[i]);
				}

				// Close connection after processing (semantics are "one connection per request")
				FD_CLR(client_fd_table[i], &allset);
				close_safe(&(client_fd_table[i]));

				if (--num_ready_fds <= 0) {
					break;
				}
			}
		}
	}

	return NULL;
}

// Returns false if stopped due to errors, true if shutdown was requested
static bool run_server_loop()
{
	// Spawn a new thread to handle client requests
	if (pthread_create(&client_thread, NULL, process_client_task, NULL)) {
		perror("run_server_loop: client thread create\n");
		return false;
	}

	// Usual preparation stuff for select()
	fd_set rset, allset;
	FD_ZERO(&allset);
	FD_SET(my_clients_fd, &allset);
	FD_SET(my_servers_fd, &allset);
	FD_SET(my_mservers_fd, &allset);

	int maxfd = max(my_clients_fd, my_servers_fd);
	maxfd = max(maxfd, my_mservers_fd);

	// Server sits in an infinite loop waiting for incoming connections from mserver/servers/client
	// and for incoming messages from already connected mserver/servers/clients
	for (;;) {
		rset = allset;

		int num_ready_fds = select(maxfd + 1, &rset, NULL, NULL, NULL);
		if (num_ready_fds < 0) {
			perror("select");
			return false;
		}

		if (num_ready_fds <= 0) {
			continue;
		}

		// Incoming connection from the metadata server
		if (FD_ISSET(my_mservers_fd, &rset)) {
			int fd_idx = accept_connection(my_mservers_fd, &mserver_fd_in, 1);
			if (fd_idx >= 0) {
				FD_SET(mserver_fd_in, &allset);
				maxfd = max(maxfd, mserver_fd_in);
			}
			assert(fd_idx == 0);

			if (--num_ready_fds <= 0) {
				continue;
			}
		}

		// Incoming connection from a key-value server
		if (FD_ISSET(my_servers_fd, &rset)) {
			int fd_idx = accept_connection(my_servers_fd, server_fd_table, 2);
			if (fd_idx >= 0) {
				FD_SET(server_fd_table[fd_idx], &allset);
				maxfd = max(maxfd, server_fd_table[fd_idx]);
			}

			if (--num_ready_fds <= 0) {
				continue;
			}
		}

		// Check for any messages from the metadata server
		if ((mserver_fd_in != -1) && FD_ISSET(mserver_fd_in, &rset)) {
			bool shutdown_requested = false;
			if (!process_mserver_message(mserver_fd_in, &shutdown_requested)) {
				// Received an invalid message, close the connection
				FD_CLR(mserver_fd_in, &allset);
				close_safe(&(mserver_fd_in));
			} else if (shutdown_requested) {
				return true;
			}

			if (--num_ready_fds <= 0) {
				continue;
			}
		}

		// Check for any messages from connected key-value servers
		for (int i = 0; i < 2; i++) {
			if ((server_fd_table[i] != -1) && FD_ISSET(server_fd_table[i], &rset)) {
				if (!process_server_message(server_fd_table[i])) {
					// Received an invalid message (or the last valid message), close the connection
					FD_CLR(server_fd_table[i], &allset);
					close_safe(&(server_fd_table[i]));
				}

				if (--num_ready_fds <= 0) {
					break;
				}
			}
		}
		if (num_ready_fds <= 0) {
			continue;
		}
	}
}


int main(int argc, char **argv)
{
	if (!parse_args(argc, argv)) {
		usage(argv);
		return 1;
	}

	open_log(log_file_name);

	if (!init_server()) {
		return 1;
	}

	run_server_loop();

	cleanup();
	return 0;
}
