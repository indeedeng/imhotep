#include <assert.h>
#include <string.h>
#include "table.h"

unpacked_table_t *unpacked_table_create(packed_table_t *packed_table, int n_rows)
{
    unpacked_table_t *table;
    int64_t *column_mins = packed_table->col_mins;
    int n_cols = packed_table_get_cols(packed_table);

    table = (unpacked_table_t *)calloc(1, sizeof(unpacked_table_t));
    table->n_rows = n_rows;
    table->n_cols = n_cols;
    table->col_offset = (int *) calloc(n_cols, sizeof(int));

    /* set for the boolean cols, padded to fit in 1 or 2 vectors */
    int offset = 0;
    int col_num;
    for (col_num = 0; col_num < packed_table->n_boolean_cols; col_num++) {
        table->col_offset[col_num] = offset;
        offset++;
    }
    /* add 1 for padding if # of booleans is odd */
    offset += packed_table->n_boolean_cols & 0x1;

    while (col_num < n_cols) {
        int vector_num = packed_table->col_2_vector[col_num];
        int cols_in_vec = packed_table->n_cols_per_vector[vector_num];
        for (int i = 0; i < cols_in_vec; i++) {
            table->col_offset[col_num] = offset;
            offset++;
            col_num++;
        }
        /* add 1 for padding if # of columns is odd */
        offset += cols_in_vec & 0x1;
    }
    /* offset should be even at this point */
    assert((offset & 0x1) == 0);
    table->unpadded_row_len = offset / 2;

    /*
     * Row size must be 1 or a multiple of 2 vectors
     * to make preloading work properly
     */
    if (table->unpadded_row_len == 1) {
        table->padded_row_len = 1;
    } else {
        /* round up to the next multiple of 2 */
        table->padded_row_len = (table->unpadded_row_len + 1) & (~0x1);
    }

    table->size = n_rows * table->padded_row_len;
    table->data = ALIGNED_ALLOC(64, sizeof(__v2di) * table->size);
    memset(table->data, 0, sizeof(__v2di) * table->size);

    /* col mins should be the size of a row. With gaps in the same places */
    table->col_mins = ALIGNED_ALLOC(64, sizeof(__v2di) * table->padded_row_len);
    memset(table->col_mins, 0, sizeof(__v2di) * table->padded_row_len);
    int64_t *table_mins = (int64_t *) table->col_mins;
    for (int i = 0; i < n_cols; i++) {
        int offset_in_row = table->col_offset[i];
        table_mins[offset_in_row] = column_mins[i];
    }

    bit_tree_init(&table->non_zero_rows, n_rows);

    return table;
}

void unpacked_table_destroy(unpacked_table_t *table)
{
    bit_tree_destroy(&table->non_zero_rows);
    free(table->col_mins);
    free(table->col_offset);
    free(table->data);
    free(table);
}


int unpacked_table_get_size(unpacked_table_t *table)
{
    return table->size;
}

int unpacked_table_get_rows(unpacked_table_t *table)
{
    return table->n_rows;
}

int unpacked_table_get_cols(unpacked_table_t *table)
{
    return table->n_cols;
}


unpacked_table_t *unpacked_table_copy_layout(unpacked_table_t *src_table, int n_rows)
{
    unpacked_table_t *new_table;

    new_table = calloc(1, sizeof(unpacked_table_t));
    new_table->n_rows = n_rows;
    new_table->n_cols = src_table->n_cols;
    new_table->padded_row_len = src_table->padded_row_len;
    new_table->unpadded_row_len = src_table->unpadded_row_len;
    new_table->col_offset = calloc(new_table->n_cols, sizeof(int));
    memcpy(new_table->col_offset, src_table->col_offset, sizeof(int) * new_table->n_cols);

    new_table->col_mins = ALIGNED_ALLOC(64, sizeof(__v2di) * new_table->padded_row_len);
    memset(new_table->col_mins, 0, sizeof(__v2di) * new_table->padded_row_len);

    new_table->size = src_table->padded_row_len * n_rows;
    new_table->data = ALIGNED_ALLOC(64, sizeof(__v2di) * new_table->size);
    memset(new_table->data, 0, sizeof(__v2di) * new_table->size);

    bit_tree_init(&new_table->non_zero_rows, n_rows);

    return new_table;
}

long unpacked_table_get_cell(unpacked_table_t *table, int row, int column)
{
    int64_t *row_data;

    row_data = (int64_t *) &table->data[table->padded_row_len * row];
    return row_data[table->col_offset[column]];
}

void unpacked_table_set_cell(unpacked_table_t *table, int row, int column, long value)
{
    int64_t *row_data;

    row_data = (int64_t *) &table->data[table->padded_row_len * row];
    row_data[table->col_offset[column]] = value;
}


static inline void core(__v2di *src_row,
                        __v2di *dest_row,
                        __v2di *mins,
                        int vector_num)
{
    dest_row[vector_num] += src_row[vector_num] + mins[vector_num];
}

inline void unpacked_table_add_rows(unpacked_table_t* src_table,
                                    int src_row_id,
                                    unpacked_table_t* dest_table,
                                    int dest_row_id,
                                    int prefetch_row_id)
{
    /* loop through row elements */
    int vector_num;
    int n_vecs = src_table->unpadded_row_len;
    __v2di *src_row = &src_table->data[src_row_id * src_table->padded_row_len];
    __v2di *dest_row = &dest_table->data[dest_row_id * dest_table->padded_row_len];
    __v2di *mins = dest_table->col_mins;
    for (vector_num = 0; vector_num < n_vecs - 4; vector_num += 4)
    {
        core(src_row, dest_row, mins, vector_num + 0);
        core(src_row, dest_row, mins, vector_num + 1);
        core(src_row, dest_row, mins, vector_num + 2);
        core(src_row, dest_row, mins, vector_num + 3);

        /* prefetch once per cache line */
        {
            __v2di *prefetch_addr = &dest_table->data[prefetch_row_id * dest_table->padded_row_len
                                                      + vector_num];
            PREFETCH(prefetch_addr);
        }
    }

    /* prefetch the final cache line */
    if (vector_num < n_vecs) {
        __v2di *prefetch_addr = &dest_table->data[prefetch_row_id * dest_table->padded_row_len
                                                  + vector_num];
        PREFETCH(prefetch_addr);
    }

    /* loop through the remaining row elements */
    for (; vector_num < n_vecs; vector_num ++)
    {
        core(src_row, dest_row, mins, vector_num);
    }
}

struct bit_tree* unpacked_table_get_non_zero_rows(unpacked_table_t* table)
{
    return &(table->non_zero_rows);
}
