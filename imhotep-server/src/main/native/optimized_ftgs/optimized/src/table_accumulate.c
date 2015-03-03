#include <stdio.h>
#include "imhotep_native.h"
#include "circ_buf.h"

#define DESC_TYPE							packed_shard_t*
#include "TwoArrayPrefetchLoop.h"

#define TGS_BUFFER_SIZE						1024
#define MAX_BIT_FIELDS						4
#define PREFETCH_CACHE_LINES				8


/*
 * process data function for unpacking data
 *
 */
static inline int unpack_bit_fields(struct circular_buffer_vector *buffer,
									uint32_t bit_fields,
									uint8_t n_bit_fields)
{
	static __m128i lookup_table[4] = { { 0L, 0L }, { 0L, 1L }, { 1L, 0L }, { 1L, 1L } };
	int i = 0;

	for (i = 0; i < n_bit_fields; i += 2) {
		circular_buffer_vector_put(buffer, lookup_table[bit_fields & 3]);
		bit_fields >>= 2;
	}
	return i / 2;
}

static inline __m128i unpack_2_metrics(__v16qi packed_data, __v16qi shuffle_vector)
{
	__m128i unpacked;

	unpacked = _mm_shuffle_epi8(packed_data, shuffle_vector);
	return unpacked;
}

static inline __v16qi row_element_extract( __v16qi* data_array,
                                           int row_size,
                                           int row_idx,
                                           int element_idx)
{
	int idx = row_idx * row_size + element_idx;
	return data_array[idx];
}

static inline void process_arrayA_data(packed_shard_t* data_desc,
                                       __v16qi data_element,
                                       struct circular_buffer_vector* stats_buf,
                                       int element_idx)
{
	struct packed_metric_desc *desc = data_desc->metrics_layout;
    uint32_t vector_index = desc->unpacked_offset[element_idx];

	uint8_t * restrict n_metrics_per_vector = desc->n_metrics_per_vector;
	__v2di *mins = (__v2di *)(desc->metric_mins);
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

static inline __m128i* calc_save_addr(packed_shard_t* data_desc,
                                        __m128i* store_array,
                                        int row_size,
                                        int row_idx,
                                        int element_idx)
{
    int true_idx = element_idx + data_desc->metrics_layout->n_boolean_metrics;
    int true_row_sz = data_desc->grp_stat_size;
	return &store_array[row_idx * true_row_sz + true_idx];
}

static inline void process_arrayB_data(packed_shard_t* data_desc,
                                       __m128i data_element,
                                       __m128i* save_buffer_start,
                                       int element_idx)
{
	*save_buffer_start += data_element;
}


static inline void arrayA_loop_core(packed_shard_t* data_desc,
                                    __v16qi* data_array,
                                    struct circular_buffer_vector* store_buf,
                                    int row_size,
                                    int row_idx,
                                    int element_idx)
{
    __v16qi data_element;
//    __m128i* save_buffer_start;

    /* load data  */
    data_element = row_element_extract(data_array, row_size, row_idx, element_idx);
//    /* calculate where to write the data */
//    int save_buf_offset = data_desc->metrics_layout->unpacked_offset[element_idx];
//    save_buffer_start = &store_array[save_buf_offset];
    /* process data */
    process_arrayA_data(data_desc, data_element, store_buf, element_idx);
}

static inline void arrayB_loop_core(packed_shard_t* data_desc,
                                    __m128i* stats_array,
                                    struct circular_buffer_vector *stats_buf,
                                    int row_size,
                                    int row_idx,
                                    int element_idx)
{
    __m128i data_element;
    __m128i *save_buffer_start;

    /* load data  */
    data_element = circular_buffer_vector_get(stats_buf);
    /* calculate where to write the data */
    save_buffer_start = calc_save_addr(data_desc, stats_array, row_size, row_idx, element_idx);
    /* process data */
    process_arrayB_data(data_desc, data_element, save_buffer_start, element_idx);
}

static inline void arrayA_rlwp(DESC_TYPE data_desc,
                               __v16qi *data_array,
                               struct circular_buffer_vector* store_buffer,
                               int row_size,
                               int row_idx,
                               int prefetch_idx)
{
    #undef LOOP_CORE
    #define LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx)      \
    {                                                                                          \
        arrayA_loop_core(data_desc, data_array, store_array, row_size, row_idx, element_idx);  \
    }
    ROW_LOOP_WITH_PREFETCH(data_desc, data_array, store_buffer, row_size, row_idx, prefetch_idx);
}

static inline void arrayA_rlnp(DESC_TYPE data_desc,
                               __v16qi *data_array,
                               struct circular_buffer_vector* store_buffer,
                               int row_size,
                               int row_idx)
{
    #undef LOOP_CORE
    #define LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx)      \
    {                                                                                          \
        arrayA_loop_core(data_desc, data_array, store_array, row_size, row_idx, element_idx);  \
    }
    ROW_LOOP_NO_PREFETCH(data_desc, data_array, store_buffer, row_size, row_idx);
}

static inline void arrayB_rlwp(DESC_TYPE data_desc,
                               struct circular_buffer_vector* stats_buf,
                               __m128i* stats_array,
                               int row_size,
                               int row_idx,
                               int prefetch_idx)
{
    #undef LOOP_CORE
    #define LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx)      \
    {                                                                                          \
        arrayB_loop_core(data_desc, data_array, store_array, row_size, row_idx, element_idx);  \
    }
    ROW_LOOP_WITH_PREFETCH(data_desc, stats_array, stats_buf, row_size, row_idx, prefetch_idx);
}

static inline void arrayB_rlnp(DESC_TYPE data_desc,
                               struct circular_buffer_vector* stats_buf,
                               __m128i* stats_array,
                               int row_size,
                               int row_idx)
{
    #undef LOOP_CORE
    #define LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx)      \
    {                                                                                          \
        arrayB_loop_core(data_desc, data_array, store_array, row_size, row_idx, element_idx);  \
    }
    ROW_LOOP_NO_PREFETCH(data_desc, stats_array, stats_buf, row_size, row_idx);
}



#define LOAD_ROW_IDX(arrayA_row_counter, doc_id_buffer)    doc_id_buffer[arrayA_row_counter]

#define LOAD_B_ROW_IDX(data_desc, data_array, row_size, row_idx, save_idx, bit_fields)     \
{                                                                                           \
	/* load group id and unpack metrics */                                                 \
	struct bit_fields_and_group packed_bf_grp;                                             \
	                                                                                       \
	/* decode bit fields */                                                                \
	packed_bf_grp = *((struct bit_fields_and_group *)&data_array[row_size * row_idx]);     \
	                                                                                       \
	/* break out group and bit fields */                                                   \
	save_idx = packed_bf_grp.grp;                                                          \
	bit_fields = packed_bf_grp.metrics;                                                    \
}


/*
 * Two array loop
 */
void prefetch_and_process_2_arrays(
                                   packed_shard_t* data_desc,
                                   __v16qi *metrics,
                                   __m128i *stats,
                                   uint32_t* doc_id_buffer,
                                   int num_rows,
                                   int gm_num_packed_vecs,
                                   int gs_num_elements,
                                   struct bit_tree *non_zero_groups,
                                   struct circular_buffer_int *grp_buf,
                                   struct circular_buffer_vector *stats_buf)
{
    /*
     * calculate the number of rows to prefetch to keep the total number
     * of prefetches to PREFETCH_CACHE_LINES
     */
    const int array_cache_lines_per_row = (gm_num_packed_vecs + 3) / 4;
    const int prefetch_rows = (PREFETCH_CACHE_LINES + array_cache_lines_per_row - 1)
                                         / array_cache_lines_per_row;

    /* loop through A rows, prefetching */
    for (int idx = 0; idx < prefetch_rows; idx++) {
        int doc_id = LOAD_ROW_IDX(idx, doc_id_buffer);
        int prefetch_idx = LOAD_ROW_IDX(idx + prefetch_rows, doc_id_buffer);

        /* load value from A, save, prefetch B */
        int prefetch_grp;
        int bit_fields;
        LOAD_B_ROW_IDX(data_desc, metrics, gm_num_packed_vecs, doc_id, prefetch_grp, bit_fields);

        /* flag group as modified */
        bit_tree_set(non_zero_groups, prefetch_grp);

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* unpack and save the bit field metrics */
        unpack_bit_fields(stats_buf, bit_fields, data_desc->metrics_layout->n_boolean_metrics);

        /* loop through A row elements */
        arrayA_rlwp(data_desc, metrics, stats_buf, gm_num_packed_vecs, doc_id, prefetch_idx);
    }

    /* loop through A rows, prefetching; loop through B rows */
    int arrayB_row_counter = 0;
    for (int idx = prefetch_rows; idx < num_rows - prefetch_rows; idx ++, arrayB_row_counter ++) {
        int doc_id = LOAD_ROW_IDX(idx, doc_id_buffer);
        int prefetch_idx = LOAD_ROW_IDX(idx + prefetch_rows, doc_id_buffer);

        /* load value from A, save, prefetch B */
        int prefetch_grp;
        int bit_fields;
        LOAD_B_ROW_IDX(data_desc, metrics, gm_num_packed_vecs, doc_id, prefetch_grp, bit_fields);

        /* flag group as modified */
        bit_tree_set(non_zero_groups, prefetch_grp);

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* unpack and save the bit field metrics */
        unpack_bit_fields(stats_buf, bit_fields, data_desc->metrics_layout->n_boolean_metrics);

        /* loop through A row elements */
        arrayA_rlwp(data_desc, metrics, stats_buf, gm_num_packed_vecs, doc_id, prefetch_idx);

        /* get load idx */
        int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        arrayB_rlwp(data_desc, stats_buf, stats, gs_num_elements, current_grp, prefetch_grp);
    }

    /* loop through A rows; loop through B rows */
    for (int idx = num_rows - prefetch_rows; idx < num_rows; idx ++, arrayB_row_counter ++) {
        int doc_id = LOAD_ROW_IDX(idx, doc_id_buffer);

        /* load value from A, save, prefetch B */
        int prefetch_grp;
        int bit_fields;
        LOAD_B_ROW_IDX(data_desc, metrics, gm_num_packed_vecs, doc_id, prefetch_grp, bit_fields);

        /* flag group as modified */
        bit_tree_set(non_zero_groups, prefetch_grp);

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* unpack and save the bit field metrics */
        unpack_bit_fields(stats_buf, bit_fields, data_desc->metrics_layout->n_boolean_metrics);

        /* loop through A row elements */
        arrayA_rlnp(data_desc, metrics, stats_buf, gm_num_packed_vecs, doc_id);

        /* get load idx */
        int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        arrayB_rlwp(data_desc, stats_buf, stats, gs_num_elements, current_grp, prefetch_grp);
    }

    /* loop through final B rows with no prefetch */
    for (; arrayB_row_counter < num_rows; arrayB_row_counter ++) {
        /* get load idx */
        int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        arrayB_rlnp(data_desc, stats_buf, stats, gs_num_elements, current_grp);
    }
}
