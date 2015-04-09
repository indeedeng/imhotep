#include <stdio.h>
#include <assert.h>
#include "imhotep_native.h"
#include "circ_buf.h"

/*
 * Two array loop
 */
void lookup_and_accumulate_grp_stats(
                                   packed_table_t *src_table,
                                   unpacked_table_t *dest_table,
                                   uint32_t* row_id_buffer,
                                   int buffer_len,
                                   struct circular_buffer_int *grp_buf,
                                   unpacked_table_t *temp_buf)
{
    /*
     * calculate the number of rows to prefetch to keep the total number
     * of prefetches to PREFETCH_CACHE_LINES
     */
    const int cache_lines_per_row = (packed_table_get_row_size(src_table) + 3) / 4;
    int prefetch_rows = (PREFETCH_DISTANCE + cache_lines_per_row - 1) / cache_lines_per_row;
    if (prefetch_rows != PREFETCH_DISTANCE) {
        /* find next higher power of 2 */
        prefetch_rows = 1 << (31 - __builtin_clz(prefetch_rows) + 1);
    }
    assert(prefetch_rows > 0);
    assert(prefetch_rows <= PREFETCH_DISTANCE);

    const int min = (buffer_len < prefetch_rows) ? buffer_len : prefetch_rows;
    const uint32_t temp_buf_mask = prefetch_rows - 1;
    int non_zero_count = 0;
    int trailing_idx = 0;
    int idx = 0;


    /* loop through A rows, prefetching */
    for (; idx < min; idx++) {
        int row_id = row_id_buffer[idx];
        int prefetch_idx = row_id_buffer[idx + prefetch_rows];

        /* load value from A, save, prefetch B */
        int prefetch_grp = packed_table_get_group(src_table, row_id);

        /* skip all group 0 docs */
        if (prefetch_grp == 0) {
        	packed_table_prefetch_row(src_table, prefetch_idx);
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
    }

    /* loop through A rows, prefetching; loop through B rows */
    for (; idx < buffer_len - prefetch_rows; idx ++) {
        int row_id = row_id_buffer[idx];
        int prefetch_idx = row_id_buffer[idx + prefetch_rows];

        /* load value from A, save, prefetch B */
        int prefetch_grp = packed_table_get_group(src_table, row_id);

        /* skip all group 0 docs */
        if (prefetch_grp == 0) {
        	packed_table_prefetch_row(src_table, prefetch_idx);
        	continue;
        }

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* loop through A row elements */
        packed_table_unpack_row_to_table(src_table,
                                         row_id,
                                         temp_buf,
                                         non_zero_count & temp_buf_mask,
                                         prefetch_idx);
        non_zero_count ++;

        /* get load idx */
        int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        unpacked_table_add_rows(temp_buf,
                                trailing_idx & temp_buf_mask,
                                dest_table,
                                current_grp,
                                prefetch_grp);
        trailing_idx ++;
    }

    /* loop through A rows; loop through B rows */
    for (; idx < buffer_len; idx ++) {
        int row_id = row_id_buffer[idx];

        /* load value from A, save, prefetch B */
        int prefetch_grp = packed_table_get_group(src_table, row_id);

        /* skip all group 0 docs */
        if (prefetch_grp == 0) {
        	continue;
        }

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* loop through A row elements */
        packed_table_unpack_row_to_table(src_table,
                                         row_id,
                                         temp_buf,
                                         non_zero_count & temp_buf_mask,
                                         row_id);
        non_zero_count ++;

        /* get load idx */
        int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        unpacked_table_add_rows(temp_buf,
                                trailing_idx & temp_buf_mask,
                                dest_table,
                                current_grp,
                                prefetch_grp);

        trailing_idx ++;
    }

    /* loop through final B rows with no prefetch */
    for (; trailing_idx < non_zero_count; trailing_idx ++) {
        /* get load idx */
        int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        unpacked_table_add_rows(temp_buf,
                                trailing_idx & temp_buf_mask,
                                dest_table,
                                current_grp,
                                current_grp);
    }
}
