#include <stdio.h>
#include "imhotep_native.h"
#include "circ_buf.h"

#define PREFETCH_CACHE_LINES                8

/*
 * Two array loop
 */
void lookup_and_accumulate_grp_stats(
                                   packed_table_t *src_table,
                                   unpacked_table_t *dest_table,
                                   uint32_t* row_id_buffer,
                                   int buffer_len,
                                   struct circular_buffer_int *grp_buf,
                                   unpacked_table_t *temp_buf,
                                   uint32_t temp_buf_mask)
{
    /*
     * calculate the number of rows to prefetch to keep the total number
     * of prefetches to PREFETCH_CACHE_LINES
     */
    const int array_cache_lines_per_row = (packed_table_get_row_size(src_table) + 3) / 4;
    const int prefetch_rows = (PREFETCH_CACHE_LINES + array_cache_lines_per_row - 1)
                                         / array_cache_lines_per_row;

    /* loop through A rows, prefetching */
    for (int idx = 0; idx < prefetch_rows; idx++) {
        int row_id = row_id_buffer[idx];
        int prefetch_idx = row_id_buffer[idx + prefetch_rows];

        /* load value from A, save, prefetch B */
        int prefetch_grp = packed_table_get_group(src_table, row_id);

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* loop through A row elements */
        packed_table_unpack_row_to_table(src_table,
                                         row_id,
                                         temp_buf,
                                         idx,
                                         prefetch_idx);
    }

    /* loop through A rows, prefetching; loop through B rows */
    int trailing_idx = 0;
    for (int idx = prefetch_rows; idx < buffer_len - prefetch_rows; idx ++, trailing_idx ++) {
        int row_id = row_id_buffer[idx];
        int prefetch_idx = row_id_buffer[idx + prefetch_rows];

        /* load value from A, save, prefetch B */
        int prefetch_grp = packed_table_get_group(src_table, row_id);

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* loop through A row elements */
        packed_table_unpack_row_to_table(src_table,
                                         row_id,
                                         temp_buf,
                                         idx & temp_buf_mask,
                                         prefetch_idx);

        /* get load idx */
        int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        unpacked_table_add_rows(temp_buf, trailing_idx, dest_table, current_grp, prefetch_grp);
    }

    /* loop through A rows; loop through B rows */
    for (int idx = buffer_len - prefetch_rows; idx < buffer_len; idx ++, trailing_idx ++) {
        int row_id = row_id_buffer[idx];

        /* load value from A, save, prefetch B */
        int prefetch_grp = packed_table_get_group(src_table, row_id);

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* loop through A row elements */
        packed_table_unpack_row_to_table(src_table, row_id, temp_buf, idx & temp_buf_mask, row_id);

        /* get load idx */
        int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        unpacked_table_add_rows(temp_buf,
                                trailing_idx & temp_buf_mask,
                                dest_table,
                                current_grp,
                                prefetch_grp);
    }

    /* loop through final B rows with no prefetch */
    for (; trailing_idx < buffer_len; trailing_idx ++) {
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
