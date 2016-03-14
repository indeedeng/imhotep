#pragma once

#include <stdlib.h>
#include <stdint.h>
#include <emmintrin.h>
#include <smmintrin.h>
#include <tmmintrin.h>
#include <pmmintrin.h>
#include "bit_tree.h"
#include <malloc.h>

#define GROUP_MASK                             0xFFFFFFF
#define MAX_BIT_FIELDS                          4
#define GROUP_SIZE                              28
#define PREFETCH_KEEP(address)                       _mm_prefetch(address, _MM_HINT_T0)
#define PREFETCH_DISCARD(address)                       _mm_prefetch(address, _MM_HINT_NTA)
//#define PREFETCH(address)

#ifdef _SANITIZE_
#define ALIGNED_ALLOC(alignment, size) malloc(size);
#else
#define ALIGNED_ALLOC(alignment, size) (((alignment) < (size)) ? memalign(alignment,size) : memalign(alignment,alignment));
#endif

struct packed_table_desc {
    int n_cols;                  /* Total number of cols */
    int n_rows;                  /* Total number of rows */
    int size;                    /* Data size in 16B vectors */
    int only_binary_columns;     /* Flag indicating table has only binary data */

    uint16_t row_size;           /* How many __m128 vectors a single row uses */
    uint16_t unpadded_row_size;  /* How many __m128 vectors a single row uses, without the end padding */
    uint16_t row_size_bytes;     /* How many bytes a single row uses */

    uint8_t* col_remapping;      /* Order stats were pushed. They are reordered to pack better */

    uint8_t *col_2_vector;       /* The vector in which the column resides */
    int64_t *col_mins;           /* The minimal value of each column */

    __v16qi *shuffle_vecs_get1;  /* shuffle vectors to get cols, 1 at a time, *NOT* counting booleans */
    __v16qi *shuffle_vecs_put;   /* shuffle vectors to put cols, 1 at a time, *NOT* counting booleans */
    __v16qi *blend_vecs_put;     /* blend vectors to blend cols, 1 at a time, *NOT* counting booleans */

    __v16qi *shuffle_vecs_get2;  /* shuffle vectors to get cols, 2 at a time, *NOT* counting booleans */

    uint8_t *n_cols_per_vector;  /* Number of column in each of the vectors, *NOT* counting booleans */

    __v16qi *data;               /* packed data */

    uint8_t n_boolean_cols;      /* Number of boolean cols */
};
typedef struct packed_table_desc packed_table_t;

struct unpacked_table_desc {
    int n_cols;             /* Total number of cols */
    int n_rows;             /* Total number of rows */
    int size;               /* Size in 16B vectors */
    int unpadded_row_len;   /* Length of a row with vector padding only. In units of 16 bytes. */
    int padded_row_len;     /* Length of a row padded out for vector and cache line alignment. in units of 16 bytes. */
    struct bit_tree *non_zero_rows;
    uint8_t *col_remapping; /* mapping of columns from original order to packed order */
    __v2di *col_mins;       /* The minimal value of each column, padded to match a row */
    int *col_offset;        /* Offset of each column in a row. In 8B longs */
    __v2di *data;           /* group and cols data packed into 128b vectors */
};
typedef struct unpacked_table_desc unpacked_table_t;

