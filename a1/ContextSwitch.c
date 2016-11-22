#define _GNU_SOURCE

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <time.h>
#include "tsc.h"
#include <asm/param.h>
#include <sched.h>
#include "common.h"

/*
Now design an experiment to measure the context switch time. The lmbench paper
describes a way to do this using pipes, however the measured context switch
time using their method will include other overheads. Your goal here is to
measure the time to switch between two (or more) ready processes. Hint:
You will need a lightly-loaded system to increase the likelihood that the
scheduler switches between only the processes that you are measuring.

Once again, create a script that can be used to run the full experiment, including
collecting, plotting, and displaying the data. Plot the activity of the processes
that you are switching between, and output the best estimate of the context switch
overhead.

How long is a time slice? That is, how long does one process get to run before it
is forced to switch to another process? Is the length of the time slice affected
by the number of processes that you are using? Are you surprised by your
measurements? How does it compare to what you were told about time slices in your
previous OS course?
*/

int main(int argc, const char *argv[])
{
  int cycles, i, s;
  int num_periods = 10;

  if (argc == 2)
  {
    num_periods = atoi(argv[1]);
  }

  u_int64_t a[num_periods * 2];
  u_int64_t astart, istart, iend;
  int fork_num = 1;

  if (set_affinity(1) == -1)
  {
    exit(1);
  }

  cycles = get_cpu_freq();

  uint64_t threshold = find_page_time();

  start_counter();

  for (i = 0; i < fork_num; i++)
  {
    pid_t pid = fork();

    if (pid < 0)
    {
      exit(1);
    }
    else if (pid == 0)
    {
      // Child process
      astart = inactive_periods(num_periods, threshold, a);
      print_output(cycles, astart, num_periods, a, " child");
      return 0;
    }
  }

  // Parent Process
  for (i = 0; i < fork_num; i++)
  {
    astart = inactive_periods(num_periods, threshold, a);
    print_output(cycles, astart, num_periods, a, " parent");
    pid_t pid = wait(&s);
  }

  return 0;
}
