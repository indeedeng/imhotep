#pragma once

#include <stdint.h>
#include <emmintrin.h>
#include <smmintrin.h>
#include <tmmintrin.h>
#include <pmmintrin.h>
#include "bit_tree.h"

#define GROUP_MASK                             0xFFFFFFF
#define MAX_BIT_FIELDS                          4
#define GROUP_SIZE                              28
#define PREFETCH(address)                       _mm_prefetch(address, _MM_HINT_T0)
#define ALIGNED_ALLOC(alignment, size) (((alignment) < (size)) ? aligned_alloc(alignment, size) : aligned_alloc(alignment,alignment));


struct bit_fields_and_group {
    uint32_t grp :28;
    uint32_t cols :4;
};

struct packed_table_desc {
    int n_cols;                  /* Total number of cols */
    int n_rows;                  /* Total number of rows */
    int size;                    /* Data size in 16B vectors */

    uint8_t n_boolean_cols;      /* Number of boolean cols */

    int row_size;                /* How many __m128 vectors a single row uses */
    int unpadded_row_size;       /* How many __m128 vectors a single row uses, without the end padding */

    uint8_t* col_remapping;     /* Order stats were pushed. They are reordered to pack better */

    uint8_t *col_2_vector;       /* The vector in which the column resides */
    int64_t *col_mins;           /* The minimal value of each column */

    __v16qi *shuffle_vecs_get1;  /* shuffle vectors to get cols, 1 at a time, *NOT* counting booleans */
    __v16qi *shuffle_vecs_put;   /* shuffle vectors to put cols, 1 at a time, *NOT* counting booleans */
    __v16qi *blend_vecs_put;     /* blend vectors to blend cols, 1 at a time, *NOT* counting booleans */

    __v16qi *shuffle_vecs_get2;  /* shuffle vectors to get cols, 2 at a time, *NOT* counting booleans */

    uint8_t *n_cols_per_vector;  /* Number of column in each of the vectors, *NOT* counting booleans */

    __v16qi *data;               /* packed data */
};
typedef struct packed_table_desc packed_table_t;

struct unpacked_table_desc {
    int n_cols;             /* Total number of cols */
    int n_rows;             /* Total number of rows */
    int size;               /* Size in 16B vectors */
    int unpadded_row_len;   /* Length of a row with vector padding only. In units of 16 bytes. */
    int padded_row_len;     /* Length of a row padded out for vector and cache line alignment. in units of 16 bytes. */
    struct bit_tree non_zero_rows;
    uint8_t *col_remapping; /* mapping of columns from original order to packed order */
    __v2di *col_mins;       /* The minimal value of each column, padded to match a row */
    int *col_offset;        /* Offset of each column in a row. In 8B longs */
    __v2di *data;           /* group and cols data packed into 128b vectors */
};
typedef struct unpacked_table_desc unpacked_table_t;

packed_table_t *packed_table_create(int n_rows,
                                    int64_t *column_mins,
                                    int64_t *column_maxes,
                                    int32_t *sizes,
                                    int32_t *vec_nums,
                                    int32_t *offsets_in_vecs,
                                    int8_t *original_idx,
                                    int n_cols);
void packed_table_destroy(packed_table_t *table);
//int packed_table_get_size(packed_table_t *table);
int packed_table_get_row_size(packed_table_t *table);
//int packed_table_get_rows(packed_table_t *table);
int packed_table_get_cols(packed_table_t *table);

//long packed_table_get_cell(packed_table_t *table, int row, int column);
//void packed_table_set_cell(packed_table_t *table, int row, int col, long value);
int packed_table_get_group(packed_table_t *table, int row);
void packed_table_set_group(packed_table_t *table, int row, int value);
void packed_table_set_all_groups(packed_table_t *table, int value);

void packed_table_batch_col_lookup( packed_table_t *table,
                                int * restrict row_ids,
                                int n_row_ids,
                                int64_t * restrict dest,
                                int column);
void packed_table_batch_set_col(   packed_table_t *table,
                            int * restrict row_ids,
                            int n_row_ids,
                            int64_t * restrict col_vals,
                            int col);
void packed_table_set_col_range(packed_table_t *table,
                                const int start_row,
                                const int64_t * restrict col_vals,
                                const int count,
                                const int col);

void packed_table_batch_group_lookup( packed_table_t *table,
                                int * restrict row_ids,
                                int n_row_ids,
                                int32_t * restrict dest);
void packed_table_batch_set_group(   packed_table_t *table,
                            int * restrict row_ids,
                            int n_row_ids,
                            int32_t * restrict group_vals);
void packed_table_set_group_range(packed_table_t *table,
                                  const int start,
                                  const int count,
                                  const int32_t * restrict group_vals);

void packed_table_unpack_row_to_table(
                                             packed_table_t* src_table,
                                             int src_row_id,
                                             unpacked_table_t* dest_table,
                                             int dest_row_id,
                                             int prefetch_row_id);

unpacked_table_t *unpacked_table_create(packed_table_t *packed_table, int n_rows);
void unpacked_table_destroy(unpacked_table_t *table);
int unpacked_table_get_rows(unpacked_table_t *table);
unpacked_table_t *unpacked_table_copy_layout(unpacked_table_t *src_table, int num_rows);

long unpacked_table_get_cell(const unpacked_table_t * restrict table,
                             const int row,
                             const int column);
void unpacked_table_set_cell(const unpacked_table_t * restrict table,
                             const int row,
                             const int column,
                             const long value);
void *unpacked_table_get_rows_addr(const unpacked_table_t * restrict table, const int row);
int64_t unpacked_table_get_remapped_cell(const unpacked_table_t *table,
                                         const int row,
                                         const int orig_idx);
void unpacked_table_add_rows(const unpacked_table_t* restrict src_table,
                                    const int src_row_id,
                                    const unpacked_table_t* restrict dest_table,
                                    const int dest_row_id,
                                    const int prefetch_row_id);
