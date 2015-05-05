/*
 * high_perf_timer.h
 *
 *  Created on: May 4, 2015
 *      Author: darren
 */

#ifndef HIGH_PERF_TIMER_H_
#define HIGH_PERF_TIMER_H_

#include <immintrin.h>

static inline void start_timer(struct worker_desc *worker, int timer_num) {
//    if (timer_num <= 3)// || timer_num == 5 || timer_num == 6)
//        worker->timings[timer_num] -= __builtin_ia32_rdtsc();
}

static inline void end_timer(struct worker_desc *worker, int timer_num) {
//    if (timer_num <= 3)// || timer_num == 5 || timer_num == 6)
//        worker->timings[timer_num] += __builtin_ia32_rdtsc();
}

#endif /* HIGH_PERF_TIMER_H_ */
