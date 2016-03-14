#include "table.h"

static int col_size_bytes(int64_t max, int64_t min, int n_cols_aux_index)
{
    uint64_t range = max - min;
    static int n_boolean_cols = 0;
    int bits = sizeof(range) * 8 - __builtin_clzl(range); /* !@# fix for zero case */

    if ((bits <= 1) && (n_boolean_cols == n_cols_aux_index)
            && (n_boolean_cols < MAX_BIT_FIELDS)) {
        n_boolean_cols++;
        return 0;
    }
    int size = ((bits - 1) / 8) + 1;
    return size;
}


int32_t *get_sizes(int n_cols, const int64_t * restrict col_mins, const int64_t * restrict col_maxes)
{
    int32_t *sizes = calloc(n_cols, sizeof(int32_t));

    for (int i = 0; i < n_cols; i++) {
        int col_size;

        col_size = col_size_bytes(col_maxes[i], col_mins[i], i);
        sizes[i] = col_size;
    }

    return sizes;
}

int32_t *get_vec_nums(
                      int n_cols,
                      const int64_t * restrict col_mins,
                      const int64_t * restrict col_maxes,
                      const int32_t * restrict sizes)
{
    int32_t *vec_nums = calloc(n_cols, sizeof(int32_t));
    int col_offset = 4;
    int n_vectors = 1;
    int packed_stats_per_vec = 0;

    /* Pack the cols and create indexes to find where they start and end */
    for (int i = 0; i < n_cols; i++) {
        int col_size = sizes[i];

        packed_stats_per_vec++;
        if (col_offset + col_size > n_vectors * 16) {
            col_offset = n_vectors * 16;
            n_vectors++;
        }
        col_offset += col_size;
        vec_nums[i] = n_vectors - 1;
    }

    return vec_nums;
}

int32_t *get_offsets_in_vecs(
                             int n_cols,
                             const int64_t * restrict col_mins,
                             const int64_t * restrict col_maxes,
                             const int32_t * restrict sizes)
{
    int32_t *offsets = calloc(n_cols, sizeof(int32_t));
    int col_offset = 4;
    int n_vectors = 1;
    int packed_stats_per_vec = 0;

    /* Pack the cols and create indexes to find where they start and end */
    for (int i = 0; i < n_cols; i++) {
        int col_size = sizes[i];

        packed_stats_per_vec++;
        if (col_offset + col_size > n_vectors * 16) {
            col_offset = n_vectors * 16;
            n_vectors++;
        }
        offsets[i] = (col_size == 0) ? 0 : col_offset % 16;    // bitfields are offset 0
        col_offset += col_size;
    }

    return offsets;
}

