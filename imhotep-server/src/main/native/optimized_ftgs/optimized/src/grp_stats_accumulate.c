#include "imhotep_native.h"
#include "circ_buf.h"

#define DESC_TYPE							packed_shard_t*
#include "TwoArrayPrefetchLoop.h"

#define TGS_BUFFER_SIZE						1024
#define MAX_BIT_FIELDS						4
#define PREFETCH_CACHE_LINES				8


//static void accumulate_stats_for_group(struct circular_buffer_int* grp_buf,
//                                       struct circular_buffer_vector* metric_buf,
//                                       __m128i* group_stats,
//                                       int n_stat_vecs_per_grp,
//                                       int grp_stat_size,
//                                       int prefetch_group_id)
//{
//	uint32_t group_id;
//
//	group_id = circular_buffer_int_get(grp_buf);
//	int load_index;
//	for (load_index = 0; load_index <= n_stat_vecs_per_grp-4; load_index += 4) {
//		__m128i stats = circular_buffer_vector_get(metric_buf);
//		group_stats[group_id * grp_stat_size + load_index + 0] += stats;
//
//		stats = circular_buffer_vector_get(metric_buf);
//		group_stats[group_id * grp_stat_size + load_index + 1] += stats;
//
//		stats = circular_buffer_vector_get(metric_buf);
//		group_stats[group_id * grp_stat_size + load_index + 2] += stats;
//
//		stats = circular_buffer_vector_get(metric_buf);
//		group_stats[group_id * grp_stat_size + load_index + 3] += stats;
//		if (prefetch_group_id != 0) {
//			_mm_prefetch(group_stats+prefetch_group_id*grp_stat_size+load_index, _MM_HINT_T0);
//		}
//	}
//
//	if (load_index < n_stat_vecs_per_grp) {
//		__m128i* prefetch_address = group_stats+prefetch_group_id*grp_stat_size+load_index;
//		do {
//			__m128i stats = circular_buffer_vector_get(metric_buf);
//			group_stats[group_id * grp_stat_size + load_index] += stats;
//			load_index++;
//		} while (load_index < n_stat_vecs_per_grp);
//		if (prefetch_group_id != 0) {
//			_mm_prefetch(prefetch_address, _MM_HINT_T0);
//		}
//	}
//}
//
//static void accumulate_stats_for_term(struct index_slice_info *slice,
//								uint32_t *doc_ids,
//								int doc_ids_len,
//								struct bit_tree *non_zero_groups,
//								__m128i *group_stats,
//								packed_shard_t *shard,
//								struct circular_buffer_int *grp_buf,
//								struct circular_buffer_vector *metric_buf)
//{
//	__v16qi* grp_metrics = shard->groups_and_metrics;
//	struct packed_metric_desc *packing_desc = shard->metrics_layout;
//	int n_vecs_per_doc = packing_desc->n_vectors_per_doc;
//
//	/* process the data */
//	for (int32_t i = 0; i < doc_ids_len; i++) {
//
//		uint32_t doc_id = doc_ids[i];
//		uint32_t start_idx = doc_id * n_vecs_per_doc;
//
//		/* load group id and unpack metrics */
//		struct bit_fields_and_group packed_bf_grp;
//		uint32_t bit_fields;
//		uint32_t group;
//
//		/* decode bit fields */
//		packed_bf_grp = *((struct bit_fields_and_group *)&grp_metrics[start_idx]);
//		bit_fields = packed_bf_grp.metrics;
//
//		/* get group*/
//		group = packed_bf_grp.grp;
//
//		/* flag group as modified */
//		bit_tree_set(non_zero_groups, group);
//
//		/* save group into buffer */
//		circular_buffer_int_put(grp_buf, group);
//
//		int prefetch_doc_id = 0;
//		if (i+PREFETCH_DISTANCE < doc_ids_len) {
//			prefetch_doc_id = doc_ids[i+PREFETCH_DISTANCE];
//		}
//		/* unpack and save the metrics for this document */
//		packed_shard_unpack_metrics_into_buffer(shard, doc_id, metric_buf, prefetch_doc_id);
//
//		if (i >= PREFETCH_DISTANCE) {
//			accumulate_stats_for_group(grp_buf, metric_buf,  group_stats, shard->n_stat_vecs_per_grp,
//			                           shard->grp_stat_size, group);
//		}
//	}    // doc id loop
//
//	/* sum the final buffered stats */
//	for (int32_t i = 0; i < PREFETCH_DISTANCE; i++) {
//		accumulate_stats_for_group(grp_buf, metric_buf, group_stats, shard->n_stat_vecs_per_grp,
//		                           shard->grp_stat_size, 0);
//	}
//}

/*
 * process data function for unpacking data
 *
 */
static inline int unpack_bit_fields(struct circular_buffer_vector *buffer,
									uint32_t bit_fields,
									uint8_t n_bit_fields)
{
	static __m128i lookup_table[4] = { { 0L, 0L }, { 0L, 1L }, { 1L, 0L }, { 1L, 1L } };
	int i;

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
                                       __m128i* save_buffer_start,
                                       int element_idx)
{
	struct packed_metric_desc *desc = data_desc->metrics_layout;
    uint32_t vector_index = desc->unpacked_offset[element_idx];

	uint8_t * restrict n_metrics_per_vector = desc->n_metrics_per_vector;
	__v2di *mins = (__v2di *)desc->metric_mins;
	__v16qi *shuffle_vecs = desc->shuffle_vecs_get2;

    for (int32_t k = 0; k < n_metrics_per_vector[element_idx]; k += 2) {
        __m128i data;
        __m128i decoded_data;

        data = unpack_2_metrics(data_element, shuffle_vecs[vector_index]);
        decoded_data = _mm_add_epi64(data, mins[vector_index]);

        /* save data into buffer */
        save_buffer_start[vector_index] = decoded_data;

        vector_index++;
    }
}

static inline __m128i* calc_save_addr(packed_shard_t* data_desc,
                                        __m128i* data_array,
                                        int row_size,
                                        int row_idx,
                                        int element_idx)
{
	return &data_array[row_idx * row_size + element_idx];
}

static inline void process_arrayB_data(packed_shard_t* data_desc,
                                       __m128i data_element,
                                       __m128i* save_buffer_start,
                                       int element_idx)
{
	*save_buffer_start += data_element;
}


#define LOAD_ROW_IDX(arrayA_row_counter, doc_id_buffer)    doc_id_buffer[arrayA_row_counter]

#define LOAD_B_ROW_IDX(data_desc, data_array, row_size, row_idx, save_idx)                 \
{                                                                                           \
	/* load group id and unpack metrics */                                                 \
	struct bit_fields_and_group packed_bf_grp;                                             \
	uint32_t group;                                                                        \
	                                                                                       \
	/* decode bit fields */                                                                \
	packed_bf_grp = *((struct bit_fields_and_group *)&data_array[row_size * row_idx]);     \
	                                                                                       \
	/* get group*/                                                                         \
	group = packed_bf_grp.grp;                                                             \
	save_idx = group;                                                                      \
}


/*
 * Two array loop
 */
void prefetch_and_process_2_arrays(
                                   packed_shard_t* data_desc,
                                   __v16qi *metrics,
                                   __m128i *stats,
                                   __m128i* temp_buffer,
                                   uint32_t* doc_id_buffer,
                                   int num_rows,
                                   int grp_metrics_row_size,
                                   int grp_stats_row_size,
                                   struct bit_tree *non_zero_groups,
                                   struct circular_buffer_int *grp_buf)
{
    /*
     * calculate the number of rows to prefetch to keep the total number
     * of prefetches to PREFETCH_CACHE_LINES
     */
    const int array_cache_lines_per_row = (grp_metrics_row_size + 3) / 4;
    const int prefetch_rows = (PREFETCH_CACHE_LINES + array_cache_lines_per_row - 1)
                                         / array_cache_lines_per_row;

    /* loop through A rows, prefetching */
    for (int doc_id_idx = 0; doc_id_idx < prefetch_rows; doc_id_idx++) {
        int doc_id = LOAD_ROW_IDX(doc_id_idx, doc_id_buffer);
        int prefetch_idx = LOAD_ROW_IDX(doc_id_idx + prefetch_rows, doc_id_buffer);

        /* load value from A, save, prefetch B */
        int prefetch_grp;
        LOAD_B_ROW_IDX(data_desc, metrics, grp_metrics_row_size, doc_id, prefetch_grp);

        /* flag group as modified */
        bit_tree_set(non_zero_groups, prefetch_grp);

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* loop through A row elements */
        arrayA_rlwp(data_desc, metrics, temp_buffer, grp_metrics_row_size, doc_id, prefetch_idx);
    }

    /* loop through A rows, prefetching; loop through B rows */
    int arrayB_row_counter = 0;
    for (int doc_id_idx = prefetch_rows;
		    doc_id_idx < num_rows - prefetch_rows;
		    doc_id_idx ++, arrayB_row_counter ++) {
        int doc_id = LOAD_ROW_IDX(doc_id_idx, doc_id_buffer);
        int prefetch_idx = LOAD_ROW_IDX(doc_id_idx + prefetch_rows, doc_id_buffer);

        /* load value from A, save, prefetch B */
        int prefetch_grp;
        LOAD_B_ROW_IDX(data_desc, metrics, grp_metrics_row_size, doc_id, prefetch_grp);

        /* flag group as modified */
        bit_tree_set(non_zero_groups, prefetch_grp);

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* loop through A row elements */
        arrayA_rlwp(data_desc, metrics, temp_buffer, grp_metrics_row_size, doc_id, prefetch_idx);

        /* get load idx */
        int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        arrayB_rlwp(data_desc, stats, NULL, grp_stats_row_size, current_grp, prefetch_grp);
    }

    /* loop through A rows; loop through B rows */
    for (int doc_id_idx = num_rows - prefetch_rows;
		    doc_id_idx < num_rows;
		    doc_id_idx ++, arrayB_row_counter ++) {
        int doc_id = LOAD_ROW_IDX(doc_id_idx, doc_id_buffer);

        /* load value from A, save, prefetch B */
        int prefetch_grp;
        LOAD_B_ROW_IDX(data_desc, metrics, grp_metrics_row_size, doc_id, prefetch_grp);

        /* flag group as modified */
        bit_tree_set(non_zero_groups, prefetch_grp);

        /* save group into buffer */
        circular_buffer_int_put(grp_buf, prefetch_grp);

        /* loop through A row elements */
        arrayA_rlnp(data_desc, metrics, temp_buffer, grp_metrics_row_size, doc_id);

        /* get load idx */
        int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        arrayB_rlwp(data_desc, stats, NULL, grp_stats_row_size, current_grp, prefetch_grp);
    }

    /* loop through final B rows with no prefetch */
    for (; arrayB_row_counter < num_rows; arrayB_row_counter ++) {
        /* get load idx */
        int current_grp = circular_buffer_int_get(grp_buf);

        /* loop through B row elements */
        arrayB_rlnp(data_desc, stats, NULL, grp_stats_row_size, current_grp);
    }
}
