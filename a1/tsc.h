#ifndef TSC_H
#define TSC_H
extern void start_counter();
extern u_int64_t get_counter();
extern u_int64_t inactive_periods(int num, u_int64_t threshold, u_int64_t *samples);
#endif
