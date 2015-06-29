#include <assert.h>
#include "imhotep_native.h"
#include "circ_buf.h"
#include "table.h"

#define GET_METRICS_FROM_MINI_BUFFER(buffer) ( ((buffer) >> ( 4 * (PREFETCH_DISTANCE - 1) )) & 0xF )


static inline struct bit_fields_and_group
binary_packed_table_get_grp_and_metrics(const packed_table_t *src_table, int row_id)
{
    struct bit_fields_and_group *data;

    data = (struct bit_fields_and_group *)src_table->data;
    return data[row_id];
}

static inline void
binary_packed_table_prefetch_row(const packed_table_t *src_table, int row_id)
{
    struct bit_fields_and_group *data;

    data = (struct bit_fields_and_group *)src_table->data;
    PREFETCH_KEEP(&data[row_id]);
}

static inline void
unpacked_table_add_binary_data(
                               const unpacked_table_t *dest_table,
                               const int current_grp,
                               const int prefetch_grp,
                               int binary_metrics)
{
    const int idx = current_grp * dest_table->padded_row_len;
    long* restrict row = (long *)&dest_table->data[idx];

    for (int i = 0; i < dest_table->n_cols; i ++) {
        row[i] += binary_metrics & 1;
        binary_metrics >>= 1;
    }

    int prefetch_idx = prefetch_grp * dest_table->padded_row_len;
    PREFETCH_KEEP(&dest_table->data[prefetch_idx]);

    /* flag row as modified */
    bit_tree_set(dest_table->non_zero_rows, current_grp);
}


/*
 * Two array loop
 */
void binary_lookup_and_accumulate_grp_stats(const struct worker_desc *worker,
                                            const packed_table_t *src_table,
                                            const unpacked_table_t *dest_table,
                                            const uint32_t* restrict row_id_buffer,
                                            const int buffer_len,
                                            const struct circular_buffer_int *grp_buf)
{
    int non_zero_count = 0;
    int trailing_idx = 0;
    int idx = 0;

    uint64_t mini_buffer = 0;

    /* loop through A rows, prefetching */
    while (non_zero_count < PREFETCH_DISTANCE && idx < buffer_len) {
        int row_id = row_id_buffer[idx];
        int prefetch_idx = row_id_buffer[idx + PREFETCH_DISTANCE];

        /* load value from A, save, prefetch B */
        const struct bit_fields_and_group data =
                binary_packed_table_get_grp_and_metrics(src_table, row_id);
        const int prefetch_grp = data.grp;
        binary_packed_table_prefetch_row(src_table, prefetch_idx);

        /* skip all group 0 docs */
        if (prefetch_grp == 0) {
            idx ++;
            continue;
        }

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* loop through A row elements */
        mini_buffer = mini_buffer << 4;
        mini_buffer |= data.metrics;

        non_zero_count ++;
        idx ++;
    }

    if (idx == buffer_len) {
        mini_buffer <<= 4 * (PREFETCH_DISTANCE - non_zero_count);
    }

    /* loop through A rows, prefetching; loop through B rows */
    for (; idx < buffer_len - PREFETCH_DISTANCE; idx ++) {
        int row_id = row_id_buffer[idx];
        int prefetch_idx = row_id_buffer[idx + PREFETCH_DISTANCE];

        /* load value from A, save, prefetch B */
        const struct bit_fields_and_group data =
                binary_packed_table_get_grp_and_metrics(src_table, row_id);
        const int prefetch_grp = data.grp;
        binary_packed_table_prefetch_row(src_table, prefetch_idx);

        /* skip all group 0 docs */
        if (prefetch_grp == 0) {
            continue;
        }

        /* get load idx */
        int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        const int binary_metrics = GET_METRICS_FROM_MINI_BUFFER(mini_buffer);
        unpacked_table_add_binary_data(dest_table, current_grp, prefetch_grp, binary_metrics);
        trailing_idx ++;

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* loop through A row elements */
        mini_buffer = mini_buffer << 4;
        mini_buffer |= data.metrics;

        non_zero_count ++;
    }

    /* loop through A rows; loop through B rows */
    for (; idx < buffer_len; idx ++) {
        int row_id = row_id_buffer[idx];

        /* load value from A, save, prefetch B */
        const struct bit_fields_and_group data =
                binary_packed_table_get_grp_and_metrics(src_table, row_id);
        const int prefetch_grp = data.grp;

        /* skip all group 0 docs */
        if (prefetch_grp == 0) {
            continue;
        }

        /* get load idx */
        int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        const int binary_metrics = GET_METRICS_FROM_MINI_BUFFER(mini_buffer);
        unpacked_table_add_binary_data(dest_table, current_grp, prefetch_grp, binary_metrics);
        trailing_idx ++;

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* loop through A row elements */
        mini_buffer = mini_buffer << 4;
        mini_buffer |= data.metrics;

        non_zero_count ++;
    }

    /* loop through final B rows with no prefetch */
    for (; trailing_idx < non_zero_count; trailing_idx ++) {
        /* get load idx */
        const int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        const int binary_metrics = GET_METRICS_FROM_MINI_BUFFER(mini_buffer);
        unpacked_table_add_binary_data(dest_table, current_grp, current_grp, binary_metrics);

        mini_buffer = mini_buffer << 4;
    }

}
