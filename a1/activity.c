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
This function continually checks the cycle counter and detects when two successive
readings differ by more than threshold cycles, which indicates that the process
has been inactive. You should determine a reasonable value to use for threshold
(tip: you don't want to record an inactive period for something like a cache or
TLB miss, nor should it be smaller than the time to collect two successive
timer samples). The start and end of the inactive period should be recorded in
samples, which is an array of 64-bit unsigned ints allocated by the caller of
the function. A total of num inactive periods should be recorded. The function
should return the initial reading - that is, the start of the first active period.
*/

/*
L1d cache:              16K
L1i cache:              64K
L2 cache:               2048K
L3 cache:               6144K

getconf PAGE_SIZE       4096
VmPTE:                  28 kB
*/

int main(int argc, const char *argv[])
{
  int num_periods = 10;

  if (argc == 2)
  {
    num_periods = atoi(argv[1]);
  }

  int i;
  double cpu = 0;
  u_int64_t a[num_periods * 2];
  uint64_t cycles = 0;
  u_int64_t astart;

  if (set_affinity(1))
  {
    exit(1);
  }

  cycles = get_cpu_freq();
  uint64_t threshold = find_page_time();
  start_counter();
  astart = inactive_periods(num_periods, threshold, a);

  print_output(cycles, astart, num_periods, a, "");

  return 0;
}
