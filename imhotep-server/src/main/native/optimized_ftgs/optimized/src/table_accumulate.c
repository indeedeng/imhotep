#include <stdio.h>
#include <assert.h>
#include "imhotep_native.h"
#include "circ_buf.h"

#include "high_perf_timer.h"

void updateGroupStats(const packed_table_t const * restrict table, long *sums, const int col)
{
    int num_rows = packed_table_get_rows(table);
    for (int i = 0; i < num_rows; i++) {
        int grp = packed_table_get_group(table, i);
        sums[grp] += packed_table_get_cell(table, i, col);
    }
}

/* Future optimization...
void updateGroupStats_2binary(const packed_table_t const * restrict table, long *sums, const int col)
{
    int num_rows = packed_table_get_rows(table);
    for (int i = 0; i < num_rows; i++) {
        int grp = packed_table_get_group(table, i);
        sums[grp] += packed_table_get_2_binary_cells(table, i, col);
    }
}
*/

/*
 * Two array loop
 */
void lookup_and_accumulate_grp_stats(
                                     const struct worker_desc *worker,
                                     const packed_table_t *src_table,
                                     const unpacked_table_t *dest_table,
                                     const uint32_t* row_id_buffer,
                                     const int buffer_len,
                                     const struct circular_buffer_int *grp_buf,
                                     const unpacked_table_t *temp_buf)
{
    /*
     * calculate the number of rows to prefetch to keep the total number
     * of prefetches to PREFETCH_DISTANCE cache lines
     */
    const int cache_lines_per_row = (packed_table_get_row_size(src_table) + 3) / 4;
    int prefetch_rows = (PREFETCH_DISTANCE + cache_lines_per_row - 1) / cache_lines_per_row;
    if (prefetch_rows != PREFETCH_DISTANCE) {
        /* find next higher power of 2 */
        prefetch_rows = 1 << (31 - __builtin_clz(prefetch_rows) + 1);
    }

    const uint32_t temp_buf_mask = prefetch_rows - 1;
    int non_zero_count = 0;
    int trailing_idx = 0;
    int idx = 0;

    /* loop through A rows, prefetching */
    while (non_zero_count < prefetch_rows && idx < buffer_len) {
        const int row_id = row_id_buffer[idx];
        const int prefetch_idx = row_id_buffer[idx + prefetch_rows];

        /* load value from A, save, prefetch B */
        const int prefetch_grp = packed_table_get_group(src_table, row_id);

        /* skip all group 0 docs */
        if (prefetch_grp == 0) {
//        	packed_table_prefetch_row(src_table, prefetch_idx);
        	idx ++;
        	continue;
        }

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* loop through A row elements */
        packed_table_unpack_row_to_table(src_table,
                                         row_id,
                                         temp_buf,
                                         non_zero_count,
                                         prefetch_idx);
        non_zero_count ++;
        idx ++;
    }

    /* loop through A rows, prefetching; loop through B rows */
    for (; idx < buffer_len - prefetch_rows; idx ++) {
        const int row_id = row_id_buffer[idx];
        const int prefetch_idx = row_id_buffer[idx + prefetch_rows];

        /* load value from A, save, prefetch B */
        const int prefetch_grp = packed_table_get_group(src_table, row_id);

        /* skip all group 0 docs */
        if (prefetch_grp == 0) {
//        	packed_table_prefetch_row(src_table, prefetch_idx);
        	continue;
        }

        /* get load idx */
        int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        unpacked_table_add_rows(temp_buf,
                                trailing_idx & temp_buf_mask,
                                dest_table,
                                current_grp,
                                prefetch_grp);
        trailing_idx ++;

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* loop through A row elements */
        packed_table_unpack_row_to_table(src_table,
                                         row_id,
                                         temp_buf,
                                         non_zero_count & temp_buf_mask,
                                         prefetch_idx);
        non_zero_count ++;
    }

    /* loop through A rows; loop through B rows */
    for (; idx < buffer_len; idx ++) {
        const int row_id = row_id_buffer[idx];

        /* load value from A, save, prefetch B */
        const int prefetch_grp = packed_table_get_group(src_table, row_id);

        /* skip all group 0 docs */
        if (prefetch_grp == 0) {
        	continue;
        }

        /* get load idx */
        int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        unpacked_table_add_rows(temp_buf,
                                trailing_idx & temp_buf_mask,
                                dest_table,
                                current_grp,
                                prefetch_grp);
        trailing_idx ++;

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* loop through A row elements */
        packed_table_unpack_row_to_table(src_table,
                                         row_id,
                                         temp_buf,
                                         non_zero_count & temp_buf_mask,
                                         row_id);
        non_zero_count ++;
    }

    /* loop through final B rows with no prefetch */
    for (; trailing_idx < non_zero_count; trailing_idx ++) {
        /* get load idx */
        const int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        unpacked_table_add_rows(temp_buf,
                                trailing_idx & temp_buf_mask,
                                dest_table,
                                current_grp,
                                current_grp);
    }
}
