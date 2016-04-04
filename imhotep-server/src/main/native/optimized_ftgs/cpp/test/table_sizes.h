#ifndef TEST_PATCH_H
#define TEST_PATCH_H

#include <stdint.h>

int32_t *get_sizes(int n_cols, const int64_t * restrict col_mins, const int64_t * restrict col_maxes);

int32_t *get_offsets_in_vecs(int n_cols,
                             const int64_t * restrict col_mins,
                             const int64_t * restrict col_maxes,
                             const int32_t * restrict sizes);

int32_t *get_vec_nums(int n_cols,
                      const int64_t * restrict col_mins,
                      const int64_t * restrict col_maxes,
                      const int32_t * restrict sizes);

#endif
