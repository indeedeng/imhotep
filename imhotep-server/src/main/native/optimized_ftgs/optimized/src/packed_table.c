#include <string.h>
#include <assert.h>
#include "table.h"

#define CACHE_LINE_SIZE                    4
#define PREFETCH_DISTANCE                  8

#define MAX(a, b) ({ \
    __typeof__(a) _a = (a); \
    __typeof__(b) _b = (b); \
    _a > _b ? _a : _b; \
})

struct bit_fields_and_group {
    uint32_t grp :28;
    uint32_t metrics :4;
};


inline uint64_t packed_table_get_2_binary_cells(const packed_table_t *table, const int row, int column)
{
    const void *packed_addr = (void *)table->data;
    const uint32_t* load_address = packed_addr + (row * table->row_size_bytes);
    const uint32_t bits_and_grp = *load_address;
    const int high_bit_num = GROUP_SIZE + 1;
    const uint64_t high_word = ((bits_and_grp & (1L << high_bit_num)) << (32  - high_bit_num));
    const uint64_t low_word = ((bits_and_grp >> GROUP_SIZE) & 0x1L);
    return high_word | low_word;
}

/* Creates the starting and end indexes of the cols, where col/16 indicates
 * in which vector the col is.
 */
static void createColumnIndexes(
                                packed_table_t *table,
                                int n_cols,
                                int32_t * restrict sizes,
                                int32_t * restrict vec_nums,
                                int32_t * restrict offsets_in_vecs,
                                uint16_t *index_cols)
{
    const uint16_t n_vectors = vec_nums[n_cols - 1] + 1;

    /* Pack the cols and create indexes to find where they start and end */
    for (int i = 0; i < n_cols; i++) {
        const int col_size  = sizes[i];

        if (col_size == 0) {
            table->n_boolean_cols ++;
        }
        index_cols[2 * i] = offsets_in_vecs[i];
        index_cols[2 * i + 1] = offsets_in_vecs[i] + col_size;
        (table->col_2_vector)[i] = vec_nums[i];
    }
    table->unpadded_row_size = n_vectors;

    /* Calculate how many cols we have per vector */
    table->n_cols_per_vector = (uint8_t *) calloc(n_vectors, sizeof(uint8_t));

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
    table->row_size_bytes = table->row_size * sizeof(__m128);
}

//Create the array that after  can be used to get 2 cols at a time from the main
//vector array.**
//**except when there is an odd number of integer cols in the vector;
static void createShuffleVecFromIndexes(packed_table_t *table, uint16_t *index_cols)
{
    uint8_t byte_vector[16];
    int k;
    const uint8_t n_boolean_cols = table->n_boolean_cols;
    const uint8_t n_cols = table->n_cols;
    const uint8_t * col_n_vector = table->col_2_vector;

    table->shuffle_vecs_get2 = calloc(n_cols - n_boolean_cols, sizeof(__v16qi));
    table->shuffle_vecs_get1 = calloc(n_cols - n_boolean_cols, sizeof(__v16qi));
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
            byte_vector[k++] = j;
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
                byte_vector[k++] = j;
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
static void createShuffleBlendFromIndexes(packed_table_t *table, uint16_t *index_cols)
{
    uint8_t byte_vector_shuffle[16];
    uint8_t byte_vector_blend[16];
    const uint8_t n_boolean_cols = table->n_boolean_cols;
    const uint8_t n_cols = table->n_cols;
    int k, i, j;
    table->shuffle_vecs_put = calloc((n_cols - n_boolean_cols), sizeof(__v16qi ));
    table->blend_vecs_put = calloc((n_cols - n_boolean_cols), sizeof(__v16qi ));

    //Creates the shuffle vectors to put each col in the right place for blending
    // And create the blend vectors. We will have a main vector that is gonna receive
    // all the cols one at a time by blending.
    // with the exception of the boolean cols.
    for (i = n_boolean_cols; i < n_cols; i++) {
        const int index = i - n_boolean_cols;
        for (j = 0; j < 16; j++) {
            byte_vector_shuffle[j] = -1;
            byte_vector_blend[j] = 0;
        }
        k = 0;
        for (j = index_cols[2 * i]; j < index_cols[2 * i + 1]; j++) {
            byte_vector_shuffle[j] = k++;
            byte_vector_blend[j] = -1;
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

packed_table_t *packed_table_create(int n_rows,
                                    int64_t *column_mins,
                                    int64_t *column_maxes,
                                    int32_t *sizes,
                                    int32_t *vec_nums,
                                    int32_t *offsets_in_vecs,
                                    int8_t *original_idx,
                                    int n_cols)
{
    packed_table_t *table;
    uint16_t index_cols[n_cols * 2];  /* Where in the vector is each column, counting booleans */

    memset(index_cols, 0, sizeof(index_cols));

    table = (packed_table_t *)calloc(1, sizeof(packed_table_t));
    table->n_rows = n_rows;
    table->n_cols = n_cols;
    table->col_remapping = calloc(n_cols, sizeof(uint8_t));
    memcpy(table->col_remapping, original_idx, n_cols * sizeof(uint8_t));

    table->col_mins = (int64_t *) calloc(n_cols, sizeof(int64_t));
    /* copy in the mins */
    for (int i = 0; i < n_cols; i++) {
        table->col_mins[i] = column_mins[i];
    }
    table->col_2_vector = (uint8_t *) calloc(n_cols, sizeof(uint8_t));
    table->n_boolean_cols = 0;
    createColumnIndexes(table,
                        n_cols,
                        sizes,
                        vec_nums,
                        offsets_in_vecs,
                        index_cols);
    if (n_cols - table->n_boolean_cols != 0) {
        createShuffleVecFromIndexes(table, index_cols);
        createShuffleBlendFromIndexes(table, index_cols);
        table->only_binary_columns = 0;
    } else {
        table->only_binary_columns = 1;
    }

    table->size = n_rows * table->row_size;
    table->data = (__v16qi *) ALIGNED_ALLOC(64, sizeof(__v16qi) * table->size);
    memset(table->data, 0, sizeof(__v16qi) * table->size);

    return table;
}

void packed_table_destroy(packed_table_t *table)
{
    table->n_rows = -1;
    table->n_cols = -1;
    table->size = -1;

    free(table->col_remapping);
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

/*
 * Attribute Getters
 */
int packed_table_is_binary_only(const packed_table_t *table)
{
    return table->only_binary_columns;
}

int packed_table_get_size(const packed_table_t *table)
{
    return table->n_rows * table->row_size;
}

int packed_table_get_row_size(const packed_table_t *table)
{
    return table->row_size;
}

int packed_table_get_rows(const packed_table_t *table)
{
    return table->n_rows;
}

int packed_table_get_cols(const packed_table_t *table)
{
    return table->n_cols;
}

__v16qi * packed_table_get_row_addr(const packed_table_t *table, int row)
{
    return &table->data[row * table->row_size];
}

/*
 * Shared cell accessors and setters
 */
static inline void internal_set_cell(const packed_table_t* table,
                                     const int row,
                                     const int col,
                                     const long value,
                                     const uint8_t row_vector_index)
{
    const size_t index = row * table->row_size;

    /* this makes the assumption that the first doc id is doc_id[0] */
    const size_t vector_index = row_vector_index + index;

    /* Converts the data to a vector and shuffles the bytes into the correct spot */
    const __m128i shuffled_col = _mm_shuffle_epi8(_mm_cvtsi64_si128(value),
                                                  table->shuffle_vecs_put[col]);

    /* Inserts the new data into the packed data vector */
    const __v16qi packed_data = table->data[vector_index];
    const __v16qi updated_data = _mm_blendv_epi8(packed_data, shuffled_col, table->blend_vecs_put[col]);

    table->data[vector_index] = updated_data;
}

static inline void internal_set_boolean_cell(const packed_table_t* table,
                                             const int row,
                                             const int col,
                                             const long value)
{
    void *packed_addr = (void *)table->data;
    uint32_t* store_address = packed_addr + (row * table->row_size_bytes);
    *store_address |= (value) << (GROUP_SIZE + col);
}

static inline int64_t internal_get_cell(const packed_table_t* table,
                                        const int row,
                                        const int column,
                                        const uint16_t row_size,
                                        const uint8_t col_vector)
{
    const __v16qi packed = (table->data)[row * row_size + col_vector];
    const __v16qi shuffle_control_vec = (table->shuffle_vecs_get1)[column];
    const __m128i unpacked = _mm_shuffle_epi8(packed, shuffle_control_vec);
    return _mm_extract_epi64(unpacked, 0);
}

static inline int internal_get_boolean_cell(const packed_table_t* table,
                                            const int row,
                                            const int column,
                                            const uint16_t row_size)
{
    const void *packed_addr = (void *)table->data;
    const uint32_t* load_address = packed_addr + (row * table->row_size_bytes);
    const uint32_t bit = (*load_address) & (1 << (GROUP_SIZE + column));
    return (bit != 0);
}

static inline int internal_get_group(const packed_table_t* table,
                                     const int row,
                                     const uint16_t row_size)
{
    const void *packed_addr = (void *)table->data;
    const struct bit_fields_and_group* packed_bf_grp = packed_addr + (row * table->row_size_bytes);
    return packed_bf_grp->grp;
}

static inline void internal_set_group(const packed_table_t* table, const int row, const int value)
{
    void *packed_addr = (void *)table->data;
    struct bit_fields_and_group* packed_bf_grp = packed_addr + (row * table->row_size_bytes);
    packed_bf_grp->grp = value & GROUP_MASK;
}


/*
 * External cell accessors and getters
 */
long packed_table_get_cell(const packed_table_t *table, const int row, int column)
{
    const uint8_t n_boolean_cols = table->n_boolean_cols;
    const int row_size = table->row_size;
    const int64_t min = (table->col_mins)[column];

    if (column >= n_boolean_cols) {
        const uint8_t col_vector = (table->col_2_vector)[column];
        column -= n_boolean_cols;
        return internal_get_cell(table, row, column, row_size, col_vector) + min;
    }

    return internal_get_boolean_cell(table, row, column, row_size) + min;
}

void packed_table_set_cell(const packed_table_t *table,
                           const int row,
                           const int col,
                           const long value)
{
    const uint8_t packed_vector_index = (table->col_2_vector)[col];
    const int64_t min = (table->col_mins)[col];

    if (col < table->n_boolean_cols) {
        internal_set_boolean_cell(table, row, col, value - min);
        return;
    }

    internal_set_cell(table, row, col, value, packed_vector_index);
}

#include <syslog.h>

int packed_table_get_num_groups(const packed_table_t *table)
{
    int result = 0;
    const int rows = packed_table_get_rows(table);
    for (int row = 0; row < rows; ++row) {
        const int group = packed_table_get_group(table, row);
        result = MAX(result, group + 1);
    }
    syslog(LOG_DEBUG, "%s: returns %d\n", __FUNCTION__, result);
    return result;
}

inline int packed_table_get_group(const packed_table_t *table, const int row)
{
    const uint16_t row_size = table->row_size;

    return internal_get_group(table, row, row_size);
}

void packed_table_set_group(const packed_table_t *table, const int row, const int value)
{
    internal_set_group(table, row, value);
}

void packed_table_set_all_groups(const packed_table_t *table, const int value)
{
    const int n_rows = table->n_rows;

    for (int row = 0; row < n_rows; row++) {
        internal_set_group(table, row, value);
    }
}


void packed_table_batch_col_lookup(const packed_table_t *table,
                                   const int * restrict row_ids,
                                   const int n_row_ids,
                                   int64_t * restrict dest,
                                   int column)
{
    const uint8_t n_boolean_cols = table->n_boolean_cols;
    const uint16_t row_size = table->row_size;
    const int64_t min = (table->col_mins)[column];

    if (column >= n_boolean_cols) {
        const uint8_t col_vector = (table->col_2_vector)[column];
        column -= n_boolean_cols;
        for (int i = 0; i < n_row_ids; i++) {
            const int row = row_ids[i];
            dest[i] = internal_get_cell(table, row, column, row_size, col_vector) + min;
        }
        return;
    }

    for (int i = 0; i < n_row_ids; i++) {
        const int row = row_ids[i];
        dest[i] = internal_get_boolean_cell(table, row, column, row_size) + min;
    }
}

void packed_table_batch_set_col(const packed_table_t *table,
                                const int * restrict row_ids,
                                const int n_row_ids,
                                const int64_t * restrict col_vals,
                                int col)
{
    const int64_t min = (table->col_mins)[col];
    const uint8_t packed_vector_index = (table->col_2_vector)[col];

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

void packed_table_set_col_range(const packed_table_t *table,
                                const int start_row,
                                const int64_t * restrict col_vals,
                                const int count,
                                int col)
{
    const int64_t min = (table->col_mins)[col];
    const uint8_t packed_vector_index = (table->col_2_vector)[col];

    if (col < table->n_boolean_cols) {
        for (int i = 0; i < count; i++) {
            const int row_id = start_row + i;
            internal_set_boolean_cell(table, row_id, col, col_vals[i] - min);

            // Prefetch
            {
                const int idx = (row_id + PREFETCH_DISTANCE) * table->row_size;
                __v16qi *prefetch_addr = &table->data[idx];
                PREFETCH_DISCARD(prefetch_addr);
            }
        }
        return;
    }

    col -= table->n_boolean_cols;
    for (int i = 0; i < count; i ++) {
        const int row_id = start_row + i;
        internal_set_cell(table, row_id, col, col_vals[i] - min, packed_vector_index);

        // Prefetch
        {
            const int idx = (row_id + PREFETCH_DISTANCE) * table->row_size;
            const __v16qi *prefetch_addr = &table->data[idx];
            PREFETCH_DISCARD(prefetch_addr);
        }
    }
}

void packed_table_batch_group_lookup(const packed_table_t *table,
                                     const int * restrict row_ids,
                                     const int n_row_ids,
                                     int32_t * restrict dest)
{
    const uint16_t row_size = table->row_size;

    for (int i = 0; i < n_row_ids; i++) {
        const int row = row_ids[i];
        dest[i] = internal_get_group(table, row, row_size);
    }
}

void packed_table_batch_set_group(const packed_table_t *table,
                                  const int * restrict row_ids,
                                  const int n_row_ids,
                                  const int32_t * restrict group_vals)
{
    for (int i = 0; i < n_row_ids; i++) {
        const int row = row_ids[i];
        internal_set_group(table, row, group_vals[i]);
    }
}

void packed_table_set_group_range(const packed_table_t *table,
                                  const int start,
                                  const int count,
                                  const int32_t * restrict group_vals)
{
    for (int i = 0; i < count; i++) {
        internal_set_group(table, start + i, group_vals[i]);
    }
}

static inline int get_bit(const long* restrict bits_arr, const int idx)
{
    const long bits = bits_arr[idx >> 6];
    const int word_idx = idx & (64 - 1);
    return bits & (0x1L << word_idx);
}

void packed_table_bit_set_regroup(const packed_table_t *table,
                                  const long* restrict bits,
                                  const int target_group,
                                  const int negative_group,
                                  const int positive_group)
{
    const int n_rows = table->n_rows;
    const uint16_t row_size = table->row_size;

    for (int row = 0; row < n_rows; ++row) {
        const int index = row * row_size;
        struct bit_fields_and_group *packed_bf_grp = (struct bit_fields_and_group *)  &table->data[index];
        const int group = packed_bf_grp->grp;

        if (group == target_group) {
            packed_bf_grp->grp = get_bit(bits, row) ? positive_group : negative_group;
        }
    }
}

void packed_table_prefetch_row(const packed_table_t *table, const int row_id)
{
	const int idx = row_id * table->row_size;
    for (int i = 0; i < table->unpadded_row_size; i += CACHE_LINE_SIZE) {
        const __v16qi *prefetch_addr = &table->data[idx + i];
        PREFETCH_DISCARD(prefetch_addr);
    }
}

/*
 *  FTGS below:
 */

static inline void unpack_bit_fields(__v2di* restrict dest_buffer,
                                    uint32_t bit_fields,
                                    uint8_t n_bit_fields)
{
    static const __v2di lookup_table[4] = { { 0L, 0L }, { 1L, 0L }, { 0L, 1L }, { 1L, 1L } };

    for (int i = 0; i < n_bit_fields; i += 2) {
        dest_buffer[i / 2] = lookup_table[bit_fields & 3];
        bit_fields >>= 2;
    }
}

static inline __m128i unpack_2_metrics(__v16qi packed_data, __v16qi shuffle_vector)
{
    const __m128i unpacked = _mm_shuffle_epi8(packed_data, shuffle_vector);
    return unpacked;
}

static inline void unpack_vector(const packed_table_t* src_table,
                                 const __v16qi vector_data,
                                 const int vector_num,
                                 const int dest_vec_num,
                                 __v2di* restrict dest_buffer)
{
    int vector_index = 0;
    const int n_cols = src_table->n_cols_per_vector[vector_num];
    const __v16qi* restrict shuffle_vecs = src_table->shuffle_vecs_get2;
    const int n_boolean_vecs = (src_table->n_boolean_cols + 1) / 2;

    for (int k = 0; k < n_cols; k += 2) {
        const __v2di data = unpack_2_metrics(vector_data, shuffle_vecs[dest_vec_num
                                                                       - n_boolean_vecs
                                                                       + vector_index]);
        /* save data into buffer */
        dest_buffer[dest_vec_num + vector_index] = data;
        vector_index++;
    }
}

static inline int core(const packed_table_t* src_table,
                       const unpacked_table_t* dest_table,
                       const int from_col,
                       const int vector_num,
                       const __v16qi* restrict src_row,
                       __v2di* restrict dest_row)
{
    const int offset_in_row = dest_table->col_offset[from_col];
    assert((offset_in_row % 2) == 0);  /* offset in row should be even */
    const int vector_offset = offset_in_row / 2;

    const __v16qi vector = src_row[vector_num];
    unpack_vector(src_table, vector, vector_num, vector_offset, dest_row);

    return src_table->n_cols_per_vector[vector_num];
}

inline void packed_table_unpack_row_to_table(const packed_table_t* src_table,
                                             const int src_row_id,
                                             const unpacked_table_t* dest_table,
                                             const int dest_row_id,
                                             const int prefetch_row_id)
{
    const __v16qi* restrict src_row = &src_table->data[src_row_id * src_table->row_size];
    __v2di* restrict dest_row = &dest_table->data[dest_row_id * dest_table->padded_row_len];

    /* unpack and save the bit field metrics */
    const struct bit_fields_and_group packed_bf_g = *((struct bit_fields_and_group *)src_row);
    unpack_bit_fields(dest_row, packed_bf_g.metrics, src_table->n_boolean_cols);

    /* return if there are only bit field columns */
    if (src_table->n_cols == src_table->n_boolean_cols) {
        /* prefetch */
        const __v16qi *prefetch_addr = &src_table->data[prefetch_row_id * src_table->row_size];
        PREFETCH_DISCARD(prefetch_addr);

        return;
    }

    /* loop through row elements */
    int vector_num;
    int column_idx = src_table->n_boolean_cols;
    const int n_packed_vecs = src_table->unpadded_row_size;
    for (vector_num = 0; vector_num < n_packed_vecs - 4; vector_num += 4)
    {
        column_idx += core(src_table, dest_table, column_idx, vector_num + 0, src_row, dest_row);
        column_idx += core(src_table, dest_table, column_idx, vector_num + 1, src_row, dest_row);
        column_idx += core(src_table, dest_table, column_idx, vector_num + 2, src_row, dest_row);
        column_idx += core(src_table, dest_table, column_idx, vector_num + 3, src_row, dest_row);

        /* prefetch once per cache line */
        {
            const __v16qi *prefetch_addr = &src_table->data[prefetch_row_id * src_table->row_size
                                                            + vector_num];
            PREFETCH_DISCARD(prefetch_addr);
        }
    }

    /* prefetch the final cache line */
    if (vector_num < n_packed_vecs) {
        const __v16qi *prefetch_addr = &src_table->data[prefetch_row_id * src_table->row_size
                                                        + vector_num];
        PREFETCH_DISCARD(prefetch_addr);
    }

    /* loop through the remaining row elements */
    for (; vector_num < n_packed_vecs; vector_num ++)
    {
        column_idx += core(src_table, dest_table, column_idx, vector_num, src_row, dest_row);
    }
}
