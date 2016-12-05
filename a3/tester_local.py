#!/usr/bin/python3

import datetime
import itertools
import random
import subprocess
import sys
import time
import threading

local = True

ssh_user = ""

local_src_path = "./"
remote_src_path = ""

client_src_path = ""
tests_path = "./"

mserver_host = "localhost"
mserver_client_port = "12346"
mserver_server_port = "23457"

mserver_config = "srvcfg_local.txt"
client_config = "clicfg_local.txt"

all_tests = "all_tests.txt"


def roundrobin(*iterables):
	"roundrobin('ABC', 'D', 'EF') --> A D E B F C"
	# Recipe credited to George Sakkis
	pending = len(iterables)
	nexts = itertools.cycle(iter(it).__next__ for it in iterables)
	while pending:
		try:
			for next in nexts:
				yield next()
		except StopIteration:
			pending -= 1
			nexts = itertools.cycle(itertools.islice(nexts, pending))


def ssh_cmd(user, host, remote_path, cmd):
	if local:
		print("running ", cmd)
		return cmd
	result = ["ssh", "-o", "StrictHostKeyChecking=no", user + "@" + host, "cd", remote_path, "&&"] + cmd
	print("running ", result)
	return result

def rsync_get_cmd(user, host, remote_path, path, exclude=[]):
	if local:
		return ["true"]
	excluded = list(itertools.chain.from_iterable(("--exclude", x) for x in exclude))
	result = ["rsync", "-aPq"] + excluded + [user + "@" + host + ":" + remote_path, path]
	if not do_commit:
		print("running ", result)
	return result

def rsync_put_cmd(user, host, path, remote_path, exclude=[]):
	if local:
		return ["true"]
	excluded = list(itertools.chain.from_iterable(("--exclude", x) for x in exclude))
	result = ["rsync", "-aPq"] + excluded + [path, user + "@" + host + ":" + remote_path]
	if not do_commit:
		print("running ", result)
	return result

def kill_arg(arg_pattern):
	return ["pkill", "-SIGKILL", "-f", arg_pattern]

def kill_cmd(process):
	return ["killall", "-s", "SIGKILL", "-q"] + [process]


def wait_process(cmd, process, check=False, timeout=None):
	thread = threading.Thread(target=lambda: process.wait())
	thread.start()
	thread.join(timeout)

	if thread.is_alive():
		process.kill()
		thread.join()
		if check:
			raise Exception("%s pid=%d timeout" % (cmd, process.pid))
		return 1
	else:
		if check and (process.returncode != 0):
			raise Exception("%s pid=%d failed with %d" % (cmd, process.pid, process.returncode))
		return process.returncode


def read_server_config(filename):
	with open(filename) as f:
		lines = f.read().splitlines()
		count = int(lines[0])
		return [l.split()[0].split('@')[-1] for l in lines[1:(count + 1)]]

def read_client_config(filename):
	with open(filename) as f:
		lines = f.read().splitlines()
		tokens = [l.split() for l in lines if l[0] != "#"]
		return list(roundrobin(*[[t[0] for i in range(int(t[1]))] for t in tokens]))

def read_test_case(filename):
	with open(filename) as f:
		lines = f.read().splitlines()
		name = lines[0]
		tokens = [l.split() for l in lines[1:] if l[0] != "#"]
		fail_period = int(tokens[0][0])
		verbose = (int(tokens[0][1]) != 0)
		client_ops = [[(t[i], int(t[i + 1]), int(t[i + 2])) for i in range(0, len(t), 3)] for t in tokens[1:]]
		return (name, fail_period, verbose, client_ops)

def read_tests_list(filename):
	with open(filename) as f:
		lines = f.read().splitlines()
		return [l for l in lines if l[0] != '#']


def kill_procs(host, name):
	subprocess.call(ssh_cmd(ssh_user, host, ".", kill_cmd(name)))

def kill_all_procs(servers, clients):
	kill_procs(mserver_host, "mserver");
	for host in servers:
		kill_procs(host, "server")
	for host in list(set(clients)):
		kill_procs(host, "client")

def cleanup_src(host):
	if not local:
		subprocess.call(ssh_cmd(ssh_user, host, ".", ["rm", "-rf", remote_src_path]))

def cleanup_all_src(servers, clients):
	cleanup_src(mserver_host)
	for host in servers:
		cleanup_src(host)
	for host in list(set(clients)):
		cleanup_src(host)

def cleanup(servers, clients):
	kill_all_procs(servers, clients)
	cleanup_all_src(servers, clients)


support_files = ["tester.py", mserver_config, client_config, all_tests]

def make_src(path, host):
	cleanup_src(host)
	if not local:
		subprocess.check_call(ssh_cmd(ssh_user, host, ".", ["mkdir", remote_src_path]))
	subprocess.check_call(rsync_put_cmd(ssh_user, host, path, remote_src_path, support_files))
	return subprocess.call(ssh_cmd(ssh_user, host, remote_src_path, ["make > make.log 2>&1"])[0], shell=True) == 0

def make_all_src(path, servers, clients):
	if not make_src(path, mserver_host):
		return False

	for host in servers:
		if not make_src(path, host):
			return False

	for host in list(set(clients)):
		if not make_src(client_src_path, host):
			return False

	return True


def put_mserver_config():
	subprocess.check_call(rsync_put_cmd(ssh_user, mserver_host, mserver_config, remote_src_path))

def start_mserver(run, verbose=True):
	cmd = ssh_cmd(ssh_user, mserver_host, remote_src_path,
	              ["./mserver", "-c", mserver_client_port, "-s", mserver_server_port, "-C", mserver_config] +
	              (["-l", "mserver_%d.log" % (run)] if verbose else []) +
	              ["1>", ("mserver_%d_stdout.log" % (run)) if verbose else "/dev/null",
	               "2>", "mserver_%d_stderr.log" % (run)])
	p = subprocess.Popen(" ".join(cmd), stdin=subprocess.PIPE, shell=True)
	return p

def stop_mserver(process):
	process.stdin.close()
	return wait_process("mserver", process, timeout=30) == 0


def put_client_ops(clients):
	for host in list(set(clients)):
		subprocess.check_call(rsync_put_cmd(ssh_user, host, tests_path, remote_src_path))

def start_client(host, id, op_file, run, verbose=True):
	cmd = ssh_cmd(ssh_user, host, remote_src_path,
	              ["./client", "-h", mserver_host, "-p", mserver_client_port, "-f", op_file] +
	              (["-l", "client_%d_%d.log" % (id, run)] if verbose else []) +
	              ["1>", ("client_%d_%d_stdout.log" % (id, run)) if verbose else "/dev/null",
	               "2>", "client_%d_%d_stderr.log" % (id, run)])
	return subprocess.Popen(" ".join(cmd), shell=True)

def stop_client(process, id, timeout=None):
	return wait_process("client_%d" % (id), process, timeout=timeout) == 0


def client_thread_f(host, id, op_file, run, count, time_limit, results, verbose=True):
	for i in range(count):
		p = start_client(host, id, op_file, run, verbose)
		if not stop_client(p, id, time_limit):
			results[id] = False
			return
	results[id] = True

killer_timer = None

def killer_thread_f(servers, period):
	global killer_timer

	sid = random.randint(0, len(servers) - 1)
	subprocess.call(ssh_cmd(ssh_user, servers[sid], ".", kill_arg("server .* -S %d .*" % (sid))))

	killer_timer = threading.Timer(period, killer_thread_f, (servers, period))
	killer_timer.start()


def print_test_result(name, output, status):
	print("<test>")
	print("\t<name>" + name + "</name>")
	print("\t<input></input>")
	print("\t<expected></expected>")
	print("\t<actual>" + output + "</actual>")
	print("\t<marks_earned>0</marks_earned>")
	print("\t<status>" + status + "</status>")
	print("</test>")


def run_test(path, servers, clients, test, run):
	global killer_timer

	name = test[0]
	fail_period = test[1]
	verbose = test[2]
	client_ops = test[3]

	mserver_process = start_mserver(run, verbose)
	time.sleep(5)
	if not (mserver_process.poll() is None):
		stop_mserver(mserver_process)
		print_test_result(
			name,
			"test %d failed, mserver failed to start, see \'*_%d.log\' files for details" % (run, run),
			"error"
		)
		return

	killer_timer = None
	if fail_period != 0:
		killer_timer = threading.Timer(1, killer_thread_f, (servers, fail_period))
		killer_timer.start()

	num_iters = max(len(ops) for ops in client_ops)
	all_results = []

	for i in range(num_iters):
		client_threads = []
		results = {}

		for id in range(len(client_ops)):
			if len(client_ops[id]) > i:
				op = client_ops[id][i]
				thread = threading.Thread(target=client_thread_f,
				                          args=(clients[id], id, op[0], run, op[1], op[2], results, verbose))
				client_threads.append(thread)

		for t in client_threads:
			t.start()
		for t in client_threads:
			t.join()
		all_results.append(results)

	if killer_timer:
		killer_timer.cancel()
		killer_timer = None

	stop_mserver(mserver_process)

	num_successes = sum(sum(1 for v in r.values() if v) for r in all_results)
	num_failures = sum(sum(1 for v in r.values() if not v) for r in all_results)
	success = (num_failures == 0) and (num_successes > 0)

	print_test_result(
		name,
		"success" if success else
			("test %d failed, total %d/%d clients failed, see \'*_%d.log\' files for details"
			 % (run, num_failures, num_failures + num_successes, run)),
		"pass" if success else "fail"
	)


def rename_server_log(host, id, run):
	subprocess.call(ssh_cmd(ssh_user, host, remote_src_path,
	                ["mv", "server_%d.log" % (id), "server_%d_%d.log" % (id, run)]))

def rename_all_server_logs(servers, run):
	for i in range(len(servers)):
		rename_server_log(servers[i], i, run)

def delete_server_log(host, id):
	subprocess.call(ssh_cmd(ssh_user, host, remote_src_path, ["rm", "-f", "server_%d.log" % (id)]))

def delete_all_server_logs(servers):
	for i in range(len(servers)):
		delete_server_log(servers[i], i)

def get_logs(host, stderr_only=False):
	if stderr_only:
		subprocess.call(rsync_get_cmd(ssh_user, host, remote_src_path + "*_stderr.log", "."))
	else:
		subprocess.call(rsync_get_cmd(ssh_user, host, remote_src_path + "*.log", "."))

def get_all_logs(servers, clients, stderr_only=False):
	get_logs(mserver_host, stderr_only)
	for host in servers:
		get_logs(host, stderr_only)
	for host in list(set(clients)):
		get_logs(host, stderr_only)


def run_all_tests(path, group_name, group_id, servers, clients, tests):
	cleanup(servers, clients)
	try:
		make_success = make_all_src(path, servers, clients)
		print_test_result(
			"make",
			"success" if make_success else "make failed, see \'make.log\' for details",
			"pass" if make_success else "error"
		)
		if not make_success:
			return False

		put_mserver_config()
		put_client_ops(clients)

		for i in range(len(tests)):
			test = read_test_case(tests_path + tests[i])
			run_test(path, servers, clients, test, i)
			if (test[2]):
				rename_all_server_logs(servers, i)
			else:
				delete_all_server_logs(servers)

	except Exception as e:
		#...
		raise

	finally:
		get_all_logs(servers, clients)
		cleanup(servers, clients)


def main():
	group_name = ""
	group_id = ""

	servers = read_server_config(mserver_config)
	clients = read_client_config(client_config)

	tests = read_tests_list(all_tests)
	run_all_tests(local_src_path, group_name, group_id, servers, clients, tests)

main()
