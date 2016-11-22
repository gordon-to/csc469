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

#define SLEEP_TIME 1E8L
#define NUM_TRIALS 5L

// http://rosettacode.org/wiki/Sorting_algorithms/Quicksort#C
void quick_sort(u_int64_t *a, u_int64_t n)
{
  u_int64_t i, j, p, t;
  if (n < 2)
    return;
  p = a[n / 2];
  for (i = 0, j = n - 1;; i++, j--)
  {
    while (a[i] < p)
      i++;
    while (p < a[j])
      j--;
    if (i >= j)
      break;
    t = a[i];
    a[i] = a[j];
    a[j] = t;
  }
  quick_sort(a, i);
  quick_sort(a + i, n - i);
}

void print_output(uint64_t cycles, u_int64_t astart, int s, u_int64_t *a, char *name)
{
  u_int64_t istart, iend;
  int i;

  for (i = 0; i < s; i++)
  {
    istart = a[2 * i];
    iend = a[(2 * i) + 1];

    printf("Active%s %d: start at %ju, duration %ju cycles (%.6Lf ms)\n", name, i, astart, (istart - astart), (long double)(istart - astart) / cycles);
    printf("Inactive%s %d: start at %ju, duration %ju cycles (%.6Lf ms)\n", name, i, istart, (iend - istart), (long double)(iend - istart) / cycles);
    astart = iend;
  }

  fflush(stdout);
}

uint64_t get_cpu_freq()
{
  struct timespec res;
  clock_getres(CLOCK_REALTIME, &res);
  struct timespec sleepTime = {0, SLEEP_TIME};
  uint64_t sleep_trial[NUM_TRIALS];
  uint64_t q, cycles = 0;

  for (q = 0; q < NUM_TRIALS; q++)
  {
    start_counter();
    if (nanosleep(&sleepTime, NULL) == 0)
    {
      sleep_trial[q] = get_counter();
      cycles += sleep_trial[q];
    }
  }

  cycles = (cycles / NUM_TRIALS);
  // printf("Clock Speed: %.2Lf GHz\n", cycles / SLEEP_TIME);
  cycles = cycles / 100;
  return cycles;
}

u_int64_t inactive_periods(int num, u_int64_t threshold, u_int64_t *samples)
{
  int i = 0;
  u_int64_t first_period, previous_period, current_period;
  first_period = current_period = previous_period = get_counter();

  while (i < num)
  {
    current_period = get_counter();
    if ((current_period - previous_period) > threshold)
    {
      samples[2 * i] = previous_period;
      samples[(2 * i) + 1] = current_period;
      i++;
    }
    previous_period = current_period;
  }

  return first_period;
}

// Print: printf("%" PRIu64 "\n", a);

uint64_t find_page_time()
{
  //long int CACHE_LINE_SIZE = 64;
  int n = (64 / sizeof(int)) * 50;
  int array[n][n];
  uint64_t a[n];
  int i, j;
  uint64_t t, t1, t2;
  srand(time(NULL));

  for (i = 0; i < n; i++)
  {
    j = rand() % n - 2;
    start_counter();
    t = get_counter();
    array[i][j]++;
    t1 = get_counter() - t;

    start_counter();
    t = get_counter();
    array[i][j + 1]++;
    t2 = get_counter() - t;

    a[i] = t1 - t2;
  }

  uint64_t delta = 0;
  quick_sort(a, n);
  int hold = n;
  for (i = 0; i < hold; i++)
  {
    if (a[i] > 10000 && a[i] < (uint64_t)100000)
    {
      //printf("%ju\n", a[i]);
      delta = delta + a[i];
    }
    else
    {
      n = n - 1;
    }
  }

  delta = delta / (uint64_t)n;
  // printf("Delta: %ju, n: %d\n", delta, n);
  return delta;
}

int set_affinity(int cpu)
{
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  return sched_setaffinity(getpid(), sizeof(cpu_set_t), &set);
}
