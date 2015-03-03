#include <stdint.h>
#include <emmintrin.h>
#include <smmintrin.h>
#include <tmmintrin.h>
#include <pmmintrin.h>

#define GROUP_SIZE                      28

struct bit_fields_and_group {
    uint32_t grp :28;
    uint32_t cols :4;
};

struct packed_table_desc {
    int n_cols;                  /* Total number of cols */
    int n_rows;                  /* Total number of rows */
    int size;                    /* Data size in 16B vectors */

    uint8_t n_boolean_cols;      /* Number of boolean cols */

    uint8_t n_cols_aux_index;    /* used to control for how many cols we have already generated the index */
    int row_size;                /* How many __m128 vectors does a single row uses */

    uint16_t *index_cols;        /* Where in the vector is each column, counting booleans */
    uint8_t *col_2_vector;       /* The vector in which the column resides */
    int64_t *col_mins;           /* The minimal value of each column */

    __v16qi *shuffle_vecs_get1;  /* shuffle vectors to get cols, 1 at a time, *NOT* counting booleans */
    __v16qi *shuffle_vecs_put;   /* shuffle vectors to put cols, 1 at a time, *NOT* counting booleans */
    __v16qi *blend_vecs_put;     /* blend vectors to blend cols, 1 at a time, *NOT* counting booleans */

    __v16qi *shuffle_vecs_get2;  /* shuffle vectors to get cols, 2 at a time, *NOT* counting booleans */

    uint8_t *n_cols_per_vector;  /* Number of column in each of the vectors, *NOT* counting booleans */

    __m128i *data;               /* packed data */
};
typedef struct packed_table_desc packed_table_t;

struct unpacked_table_desc {
    int n_cols;             /* Total number of cols */
    int n_rows;             /* Total number of rows */
    int size;               /* Size in 16B vectors */
    int unpadded_row_len;   /* Length of a row with vector padding only. In units of 16 bytes. */
    int padded_row_len;     /* Length of a row padded out for vector and cache line alignment. in units of 16 bytes. */
    __v2di *col_mins;       /* The minimal value of each column, padded to match a row */
    uint8_t *col_offset;    /* Offset of each column in a row. In 8B longs */
    __v16qi *data;          /* group and cols data packed into 128b vectors */
};
typedef struct unpacked_table_desc unpacked_table_t;


//This method assumes that the boolean cols will come first
//packed_table_t *packed_table_create(uint32_t n_rows,
//                                    int64_t *column_mins,
//                                    int64_t *column_maxes,
//                                    int n_cols);
//int packed_table_get_size(packed_table_t *table);
//int packed_table_get_rows(packed_table_t *table);
//int packed_table_get_cols(packed_table_t *table);
//long packed_table_get_cell(packed_table_t *table, int row, int col);
//void packed_table_set_cell(packed_table_t *table, int row, int col, long value);
//void packed_table_unpack_row(packed_table_t *src,
//                             int src_row,
//                             unpacked_table_t *dest,
//                             int dest_row);
//
//struct unpacked_table_t *unpacked_table_create();
//int unpacked_table_get_size(unpacked_table_t *table);
//int unpacked_table_get_rows(unpacked_table_t *table);
//int unpacked_table_get_cols(unpacked_table_t *table);
//struct unpacked_table_t *unpacked_table_copy_layout(unpacked_table_t *src_table, int new_size);
//long unpacked_table_get_cell(unpacked_table_t *table, int row, int column);
//void unpacked_table_set_cell(unpacked_table_t *table, int row, int column, long value);


int const GROUP_MASK = 0xFFFFFFF;
#define MAX_BIT_FIELDS                  4


static int col_size_bytes(packed_table_t *table, int64_t max, int64_t min)
{
    uint64_t range = max - min;
    int bits = sizeof(range) * 8 - __builtin_clzl(range); /* !@# fix for zero case */
    if ((bits <= 1) && (table->n_boolean_cols == table->n_cols_aux_index)
            && (table->n_boolean_cols < MAX_BIT_FIELDS)) {
        table->n_boolean_cols++;
        return 0;
    }
    int size = ((bits - 1) / 8) + 1;
    return size;
}

/* Creates the starting and end indexes of the cols, where col/16 indicates
 * in which vector the col is.
 */
static void createColumnIndexes(
                                packed_table_t *table,
                                int n_cols,
                                int64_t * restrict col_mins,
                                int64_t * restrict col_maxes,
                                uint8_t first_free_byte)
{
    int col_offset = first_free_byte;    //Find the initial byte for the cols.
    int n_vectors = 1;
    int grp_stats_long_count = 0;
    int packed_stats_per_vec = 0;

    /* Pack the cols and create indexes to find where they start and end */
    for (int i = 0; i < n_cols; i++) {
        int col_size;

        col_size = col_size_bytes(table, col_maxes[i], col_mins[i]);
        packed_stats_per_vec ++;
        if (col_offset + col_size > n_vectors * 16) {
            col_offset = n_vectors * 16;
            n_vectors++;
            if ((packed_stats_per_vec & 0x1) == 1) {
                grp_stats_long_count ++;
            }
        }
        (table->index_cols)[2 * i] = col_offset;
        col_offset += col_size;
        (table->index_cols)[2 * i + 1] = col_offset;
        (table->col_2_vector)[i] = n_vectors - 1;
        table->n_cols_aux_index++;

        grp_stats_long_count ++;
    }

    /* Calculate how many cols we have per vector */
    table->n_cols_per_vector = (uint8_t *) calloc(sizeof(uint8_t), n_vectors);

    /* Count how many non-bitfield cols in each packed vector */
    for (int i = table->n_boolean_cols; i < n_cols; i++) {
        (table->n_cols_per_vector)[(table->col_2_vector)[i]]++;
    }

    /*
     * Group cols row size must be 1 or a multiple of 2 vectors
     * to make preloading work properly
     */
    if (n_vectors == 1) {
        table->row_size = n_vectors;
    } else {
        /* round up to the next multiple of 2 */
        table->row_size = (n_vectors + 1) & (~0x1);
    }
}

//Create the array that after  can be used to get 2 cols at a time from the main
//vector array.**
//**except when there is an odd number of integer cols in the vector;
static void createShuffleVecFromIndexes(packed_table_t *table)
{
    uint8_t byte_vector[16];
    int k;
    uint8_t n_boolean_cols = table->n_boolean_cols;
    uint8_t n_cols = table->n_cols;
    uint16_t * index_cols = table->index_cols;
    uint8_t * col_n_vector = table->col_2_vector;

    table->shuffle_vecs_get2 = calloc(sizeof(__v16qi), n_cols - n_boolean_cols);
    table->shuffle_vecs_get1 = calloc(sizeof(__v16qi), n_cols - n_boolean_cols);
    int index2 = 0;
    int index1 = 0;

    for (int i = n_boolean_cols; i < n_cols; i++) {

        //clears the vector
        for (int j = 0; j < 16; j++) {
            byte_vector[j] = -1;
        }
        //creates the first part of the __m128i vector
        k = 0;
        for (int j = index_cols[2 * i]; j < index_cols[2 * i + 1]; j++) {
            byte_vector[k++] = j % 16;
        }
        table->shuffle_vecs_get1[index1++] = _mm_setr_epi8(byte_vector[0],
                                                byte_vector[1],
                                                byte_vector[2],
                                                byte_vector[3],
                                                byte_vector[4],
                                                byte_vector[5],
                                                byte_vector[6],
                                                byte_vector[7],
                                                -1,
                                                -1,
                                                -1,
                                                -1,
                                                -1,
                                                -1,
                                                -1,
                                                -1);

        //creates the second part of the vector, if the other col is in the same vector
        //as the first col. If not, just put -1s in this half of shuffle_vecs_get2
        k = 8;
        i++;
        if (i < n_cols && (col_n_vector[i] == col_n_vector[i - 1])) {
            for (int j = index_cols[2 * i]; j < index_cols[2 * i + 1]; j++) {
                byte_vector[k++] = j % 16;
            }
            table->shuffle_vecs_get1[index1++] = _mm_setr_epi8(byte_vector[8],
                                                    byte_vector[9],
                                                    byte_vector[10],
                                                    byte_vector[11],
                                                    byte_vector[12],
                                                    byte_vector[13],
                                                    byte_vector[14],
                                                    byte_vector[15],
                                                    -1,
                                                    -1,
                                                    -1,
                                                    -1,
                                                    -1,
                                                    -1,
                                                    -1,
                                                    -1);
        } else {
            i--;
        }
        table->shuffle_vecs_get2[index2++] = _mm_setr_epi8(byte_vector[0],
                                                byte_vector[1],
                                                byte_vector[2],
                                                byte_vector[3],
                                                byte_vector[4],
                                                byte_vector[5],
                                                byte_vector[6],
                                                byte_vector[7],
                                                byte_vector[8],
                                                byte_vector[9],
                                                byte_vector[10],
                                                byte_vector[11],
                                                byte_vector[12],
                                                byte_vector[13],
                                                byte_vector[14],
                                                byte_vector[15]);
    }
}

//Creates the shuffle and Blends vectors used to put cols inside the vector.
static void createShuffleBlendFromIndexes(packed_table_t *table)
{
    uint8_t byte_vector_shuffle[16];
    uint8_t byte_vector_blend[16];
    uint8_t n_boolean_cols = table->n_boolean_cols;
    uint8_t n_cols = table->n_cols;
    uint16_t * index_cols = table->index_cols;
    int k, i, j;
    table->shuffle_vecs_put = calloc(sizeof(__v16qi ), (n_cols - n_boolean_cols));
    table->blend_vecs_put = calloc(sizeof(__v16qi ), (n_cols - n_boolean_cols));

    //Creates the shuffle vectors to put each col in the right place for blending
    // And create the blend vectors. We will have a main vector that is gonna receive
    // all the cols one at a time by blending.
    // with the exception of the boolean cols.
    for (i = n_boolean_cols; i < n_cols; i++) {
        int index = i - n_boolean_cols;
        for (j = 0; j < 16; j++) {
            byte_vector_shuffle[j] = -1;
            byte_vector_blend[j] = 0;
        }
        k = 0;
        for (j = index_cols[2 * i]; j < index_cols[2 * i + 1]; j++) {
            byte_vector_shuffle[j % 16] = k++;
            byte_vector_blend[j % 16] = -1;
        }
        table->shuffle_vecs_put[index] = _mm_setr_epi8(  byte_vector_shuffle[0],
                                                byte_vector_shuffle[1],
                                                byte_vector_shuffle[2],
                                                byte_vector_shuffle[3],
                                                byte_vector_shuffle[4],
                                                byte_vector_shuffle[5],
                                                byte_vector_shuffle[6],
                                                byte_vector_shuffle[7],
                                                byte_vector_shuffle[8],
                                                byte_vector_shuffle[9],
                                                byte_vector_shuffle[10],
                                                byte_vector_shuffle[11],
                                                byte_vector_shuffle[12],
                                                byte_vector_shuffle[13],
                                                byte_vector_shuffle[14],
                                                byte_vector_shuffle[15]);

        table->blend_vecs_put[index] = _mm_setr_epi8(    byte_vector_blend[0],
                                            byte_vector_blend[1],
                                            byte_vector_blend[2],
                                            byte_vector_blend[3],
                                            byte_vector_blend[4],
                                            byte_vector_blend[5],
                                            byte_vector_blend[6],
                                            byte_vector_blend[7],
                                            byte_vector_blend[8],
                                            byte_vector_blend[9],
                                            byte_vector_blend[10],
                                            byte_vector_blend[11],
                                            byte_vector_blend[12],
                                            byte_vector_blend[13],
                                            byte_vector_blend[14],
                                            byte_vector_blend[15]);
    }
}

//This method assumes that the boolean cols will come first
packed_table_t *packed_table_create(uint32_t n_rows,
                                    int64_t *column_mins,
                                    int64_t *column_maxes,
                                    int n_cols)
{
    packed_table_t *table;

    table = (struct packed_table_t *)calloc(sizeof(struct packed_table_t), 1);
    table->n_rows = n_rows;
    table->n_cols = n_cols;

    table->col_mins = (int64_t *) calloc(sizeof(int64_t), n_cols);
    /* copy in the mins */
    for (int i = 0; i < n_cols; i++) {
        table->col_mins[i] = column_mins[i];
    }
    table->index_cols = (uint16_t *) calloc(sizeof(uint16_t), n_cols * 2);
    table->col_2_vector = (uint8_t *) calloc(sizeof(uint8_t), n_cols);
    table->n_cols_aux_index = 0;
    table->n_boolean_cols = 0;
    createColumnIndexes(   table,
                        n_cols,
                        column_mins,
                        column_maxes,
                        (GROUP_SIZE + MAX_BIT_FIELDS + 7) / 8);
    createShuffleVecFromIndexes(table);
    createShuffleBlendFromIndexes(table);

    table->size = n_rows * table->row_size;
    table->data = (__v16qi *) aligned_alloc(64, sizeof(__m128i ) * table->size);
    memset(table->data, 0, sizeof(__m128i ) * table->size);

    return table;
}

unpacked_table_t *unpacked_table_create(packed_table_t *packed_table,
                                        int64_t *column_mins,
                                        int64_t *column_maxes,
                                        int n_rows)
{
    unpacked_table_t *table;
    int n_cols = packed_table_get_cols(packed_table);

    table->col_offset = (uint8_t *) calloc(sizeof(uint8_t), n_cols);

    /* set for the boolean cols, padded to fit in 1 or 2 vectors */
    uint8_t offset = 0;
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
    table->padded_row_len = offset / 2;

    /*
     * Row size must be 1 or a multiple of 2 vectors
     * to make preloading work properly
     */
    if (table->padded_row_len == 1) {
        table->unpadded_row_len = 1;
    } else {
        /* round up to the next multiple of 2 */
        table->unpadded_row_len = (table->padded_row_len + 1) & (~0x1);
    }

    /* col mins should be the size of a row. With gaps in the same places */
    table->col_mins = aligned_alloc(16, sizeof(__v2di) * table->padded_row_len);
    memset(table->col_mins, 0, sizeof(__v2di) * table->padded_row_len);
    for (int i = 0; i < n_cols; i++) {
        int offset_in_row = table->col_offset[i];
        table->col_mins[offset_in_row] = column_mins[i];
    }

    return table;
}


void packed_table_destroy(packed_table_t *table)
{
    struct packed_col_desc *desc;

    table->n_rows = -1;
    table->n_cols = -1;
    table->size = -1;

    free(table->index_cols);
    free(table->col_2_vector);
    free(table->col_mins);
    free(table->shuffle_vecs_put);
    free(table->blend_vecs_put);
    free(table->shuffle_vecs_get1);
    free(table->shuffle_vecs_get2);
    free(table->n_cols_per_vector);
    free(table->data);
    free(table);
}

int packed_table_get_size(packed_table_t *table)
{
    return table->n_rows * table->row_size;
}

int packed_table_get_rows(packed_table_t *table)
{
    return table->n_rows;
}

int packed_table_get_cols(packed_table_t *table)
{
    return table->n_cols;
}

long packed_table_get_cell(packed_table_t *table, int row, int column)
{
    uint8_t n_boolean_cols = table->n_boolean_cols;
    int row_size = table->row_size;
    int64_t min = (table->col_mins)[column];

    if (column >= n_boolean_cols) {
        uint8_t col_vector = (table->col_2_vector)[column];
        column -= n_boolean_cols;
        return internal_get_cell(table, row, column, row_size, col_vector) + min;
    }

    if (column == 0) {
        return internal_get_col_0(table, row, row_size) + min;
    }

    return internal_get_boolean_cell(table, row, column, row_size) + min;
}

void internal_set_cell(
                       packed_table_t* table,
                       int row,
                       int col,
                       long value,
                       uint8_t row_vector_index)
{
    size_t index = row * table->row_size;

    /* this makes the assumption that the first doc id is doc_id[0] */
    size_t vector_index = row_vector_index + index;

    /* Converts the data to a vector and shuffles the bytes into the correct spot */
    __m128i shuffled_col = _mm_shuffle_epi8(_mm_cvtsi64_si128(value),
                                            table->shuffle_vecs_put[col]);

    /* Inserts the new data into the packed data vector */
    __v16qi packed_data = table->data[vector_index];
    __v16qi updated_data = _mm_blendv_epi8(packed_data, shuffled_col, table->blend_vecs_put[col]);

    table->data[vector_index] = updated_data;
}

void internal_set_boolean_cell(packed_table_t* table,
                               int row,
                               int col,
                               long value,
                               uint8_t row_vector_index,
                               int64_t min)
{
    __v16qi *packed_addr;
    uint32_t *store_address;
    int index = col * table->row_size + 0;

    packed_addr = &(table->data[index]);
    store_address = (uint32_t *)packed_addr;
    *store_address |= (value - min) << (GROUP_SIZE + col);
}


void packed_table_set_cell(packed_table_t *table, int row, int col, long value)
{
    uint8_t packed_vector_index = (table->col_2_vector)[col];
    int64_t min = (table->col_mins)[col];

    if (col < table->n_boolean_cols) {
        internal_set_boolean_cell(table, row, col, value);
        return;
    }

    internal_set_cell(table, row, col, value, packed_vector_index);
}


void packed_shard_batch_set_col(   packed_table_t *table,
                            int * restrict row_ids,
                            int n_row_ids,
                            int64_t * restrict col_vals,
                            int col)
{
    int64_t min = (table->col_mins)[col];
    uint8_t packed_vector_index = (table->col_2_vector)[col];

    if (col == 0) {
        for (int i = 0; i < n_row_ids; i++) {
            int row = row_ids[i];
            internal_set_col_0(table, row, col_vals[i] - min);
        }
        return;
    }

    if (col < table->n_boolean_cols) {
        for (int i = 0; i < n_row_ids; i++) {
            internal_set_boolean_cell(table, row_ids[i], col, col_vals[i] - min);
        }
        return;
    }

    col -= table->n_boolean_cols;
    for (int i = 0; i < n_row_ids; i++) {
        internal_set_cell(table, row_ids[i], col, col_vals[i] - min, packed_vector_index);
    }
}

int64_t internal_get_cell(
                          packed_table_t* table,
                          int row,
                          int column,
                          int row_size,
                          uint8_t col_vector)
{
    __v16qi packed;
    __v16qi shuffle_control_vec;
    __m128i unpacked;
    int64_t result;
    packed = (table->data)[row * row_size + col_vector];
    shuffle_control_vec = (table->shuffle_vecs_get1)[column];
    unpacked = _mm_shuffle_epi8(packed, shuffle_control_vec);
    result = _mm_extract_epi64(unpacked, 0);
    return result;
}

int internal_get_boolean_cell(
                             packed_table_t* table,
                             int row,
                             int column,
                             int row_size)
{
    __v16qi* packed_addr;
    uint32_t* load_address;
    uint32_t bit;

    int index = row * row_size + 0;
    packed_addr = &table->data[index];
    load_address = (uint32_t*)packed_addr;
    bit = (*load_address) & (1 << (GROUP_SIZE + column));

    return (bit != 0);
}

void packed_shard_batch_col_lookup( packed_table_t *table,
                                int * restrict row_ids,
                                int n_row_ids,
                                int64_t * restrict dest,
                                int column)
{
    uint8_t n_boolean_cols = table->n_boolean_cols;
    int row_size = table->row_size;
    int64_t min = (table->col_mins)[column];

    if (column >= n_boolean_cols) {
        uint8_t col_vector = (table->col_2_vector)[column];
        column -= n_boolean_cols;
        for (int i = 0; i < n_row_ids; i++) {
            int row = row_ids[i];
            dest[i] = internal_get_cell(table, row, column, row_size, col_vector) + min;
        }
        return;
    }

    if (column == 0) {
        for (int i = 0; i < n_row_ids; i++) {
            int row = row_ids[i];
            dest[i] = internal_get_col_0(table, row, row_size) + min;
        }
        return;
    }

    for (int i = 0; i < n_row_ids; i++) {
        int row = row_ids[i];
        dest[i] = internal_get_boolean_cell(table, row, column, row_size) + min;
    }
}


int internal_get_col_0(
                        packed_table_t* table,
                        int row,
                        int row_size)
{
    __v16qi *packed_addr;
    const struct bit_fields_and_group *packed_bf_grp;
    const int index = row * row_size;

    packed_addr = &table->data[index];
    packed_bf_grp = (struct bit_fields_and_group *) packed_addr;
    return packed_bf_grp->grp;
}

void internal_set_col_0(
                        packed_table_t* table,
                        int row,
                        int value)
{
    __v16qi *packed_addr;
    struct bit_fields_and_group *packed_bf_grp;
    const size_t index = row * table->row_size;

    packed_addr = &table->data[index];
    packed_bf_grp = (struct bit_fields_and_group *)packed_addr;
    packed_bf_grp->grp = value & GROUP_MASK;
}

static inline void packed_table_unpack_row_to_table(
                                                    packed_table_t* src_table,
                                                    int row,
                                                    unpacked_table_t* dest_table)
{
    /* loop through row elements */
    int element_idx;
    for (element_idx = 0; element_idx < row_size - 4; element_idx += 4)
    {
        LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx + 0);

        LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx + 1);

        LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx + 2);

        LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx + 3);

        /* prefetch once per cache line */
        PREFETCH(data_array, row_size, prefetch_idx, element_idx);
    }

    /* prefetch the final cache line */
    if (element_idx < row_size) {
        PREFETCH(data_array, row_size, prefetch_idx, element_idx);
    }
    /* loop through the remaining row elements */
    for (; element_idx < row_size; element_idx ++)
    {
        LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx);
    }
}

void unpackme()
{
    int
    uint8_t * restrict n_metrics_per_vector = desc->n_metrics_per_vector;
    __v2di *mins = (__v2di *)(dest_table->metric_mins);
    __v16qi *shuffle_vecs = desc->shuffle_vecs_get2;

    for (int32_t k = 0; k < n_metrics_per_vector[element_idx]; k += 2) {
        __m128i data;
        __m128i decoded_data;

        data = unpack_2_metrics(data_element, shuffle_vecs[vector_index]);
        decoded_data = _mm_add_epi64(data, mins[vector_index]);
        vector_index++;

        /* save data into buffer */
        circular_buffer_vector_put(stats_buf, decoded_data);
    }
}

/*
 *
 *
 * Unpacked Table
 *
 *
 *
 */
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

struct unpacked_table_t *unpacked_table_copy_layout(unpacked_table_t *src_table, int num_rows)
{
    unpacked_table_t *new_table;

    new_table = calloc(sizeof(unpacked_table_t), 1);
    new_table->n_rows = num_rows;
    new_table->n_cols = src_table->n_cols;
    new_table->padded_row_len = src_table->padded_row_len;
    new_table->unpadded_row_len = src_table->unpadded_row_len;
    new_table->col_offset = calloc(sizeof(uint8_t), new_table->n_cols);
    memcpy(new_table->col_offset, src_table->col_offset, sizeof(uint8_t) * new_table->n_cols);

    new_table->size = src_table->padded_row_len * num_rows;
    new_table->data = aligned_alloc(64, new_table->size);
    memset(new_table->data, 0, new_table->size);

    return new_table;
}

long unpacked_table_get_cell(unpacked_table_t *table, int row, int column)
{
    int64_t *row_data;

    row_data = &table->data[table->padded_row_len * row];
    return row_data[table->col_offset[column]];
}

void unpacked_table_set_cell(unpacked_table_t *table, int row, int column, long value)
{
    int64_t *row_data;

    row_data = &table->data[table->padded_row_len * row];
    row_data[table->col_offset[column]] = value;
}

