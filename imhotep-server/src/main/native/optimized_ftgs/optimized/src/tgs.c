#include "imhotep_native.h"
#include "circ_buf.h"
#include "varintdecode.h"

#define TGS_BUFFER_SIZE						1024
#define MAX_BIT_FIELDS						4
#define PREFETCH_CACHE_LINES				8


/* No need to share the group stats buffer, so just keep one per session*/
/* Make sure the one we have is large enough */
static __m128i *allocate_grp_stats(struct worker_desc *desc, 
							struct session_desc *session)
{
	int num_cells = session->num_groups * session->num_stats;

	if (desc->group_stats_buf == NULL) {
		desc->buffer_size = sizeof(uint64_t) * num_cells;
		desc->group_stats_buf = (__m128i *)calloc(sizeof(uint64_t), num_cells);
		return desc->group_stats_buf;
	}
	
	if (desc->buffer_size >= (sizeof(uint64_t) * num_cells)) {
		// our buffer is large enough already;
		return desc->group_stats_buf;
	}
	
	free(desc->group_stats_buf);
	// TODO: maybe resize smarter
	desc->buffer_size = sizeof(uint64_t) * num_cells;
	desc->group_stats_buf = (__m128i *)calloc(sizeof(uint64_t), num_cells);
	return desc->group_stats_buf;
}

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
static inline __m128i row_element_extract( __m128i* data_array,
                                           int row_size,
                                           int row_idx,
                                           int element_idx)
{
	int idx = row_idx * row_size + element_idx;
	return data_array[idx];
}

static inline void process_arrayA_data(packed_shard_t* data_desc,
                                       __m128i data_element,
                                       __m128i* save_buffer_start,
                                       int element_idx)
{
	struct packed_metric_desc *desc = data_desc->metrics_layout;
    uint32_t vector_index = desc->unpacked_offset[element_idx];

	uint32_t * restrict n_metrics_per_vector = desc->n_metrics_per_vector;
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

static inline __m128i* calc_save_addr(packed_shard_t* data_desc, int row_idx, int element_idx)
{
	return &group_stats[row_idx * grp_stat_size + element_idx];
}

static inline void process_arrayB_data(packed_shard_t* data_desc, __m128i data_element,
                                       __m128i* save_buffer_start, int element_idx)
{
	*save_buffer_start += data_element;
}


#define LOAD_ROW_IDX(arrayA_row_counter, doc_id_buffer)    doc_id_buffer[arrayA_row_counter]

#define LOAD_B_ROW_IDX(data_desc, data_array, row_size, row_idx, save_idx)                 \
{                                                                                          \
	/* load group id and unpack metrics */                                                 \
	struct bit_fields_and_group packed_bf_grp;                                             \
	uint32_t group;                                                                        \
	                                                                                       \
	/* decode bit fields */                                                                \
	packed_bf_grp = *((struct bit_fields_and_group *)&data_array[row_size * row_idx]);     \
	                                                                                       \
	/* get group*/                                                                         \
	group = packed_bf_grp.grp;                                                             \
	                                                                                       \
	/* flag group as modified */                                                           \
	bit_tree_set(non_zero_groups, group);                                                  \
	                                                                                       \
	save_idx = group;                                                                      \
}

#define DESC_TYPE       packed_shard_t*


#include "TwoArrayPrefetchLoop.h"


void tgs_init(struct worker_desc *worker,
              struct tgs_desc *desc,
              uint8_t term_type,
              union term_union *term,
              union term_union *previous_term,
              long *addresses,
              int *docs_per_shard,
              int *shard_handles,
              int num_shard,
              struct buffered_socket *socket,
              struct session_desc *session)
{
	struct index_slice_info *infos;
	desc->term_type = term_type;
	desc->term = term;
	desc->previous_term = previous_term;
	desc->n_slices = num_shard;
	desc->socket = socket;
	infos = (struct index_slice_info *)
			calloc(sizeof(struct index_slice_info), num_shard);
	for (int i = 0; i < num_shard; i++) {
		int handle = shard_handles[i];
		infos[i].n_docs_in_slice = docs_per_shard[i];
		infos[i].slice = (uint8_t *)addresses[i];
		infos[i].shard = &(session->shards[handle]);
	}
	desc->trm_slice_infos = infos;
	desc->grp_buf = worker->grp_buf;
	desc->metric_buf = worker->metric_buf;
	desc->non_zero_groups = worker->bit_tree_buf;
}

void tgs_destroy(struct tgs_desc *desc)
{
	bit_tree_destroy(desc->non_zero_groups);
	free(desc->trm_slice_infos);
}

int tgs_execute_pass(struct worker_desc *worker,
                     struct session_desc *session,
                     struct tgs_desc *desc)
{
	uint32_t doc_id_buf[TGS_BUFFER_SIZE];
	__m128i *group_stats;
	int n_slices = desc->n_slices;
	struct index_slice_info *infos = desc->trm_slice_infos;

	group_stats = allocate_grp_stats(worker, session);
	session->current_tgs_pass->group_stats = group_stats;

	for (int i = 0; i < n_slices; i++) {
		struct index_slice_info *slice;
		int remaining;      /* num docs remaining */
		uint8_t *read_addr;
		int last_value;     /* delta decode tracker */

		slice = &infos[i];
		remaining = slice->n_docs_in_slice;
		read_addr = slice->slice;
		last_value = 0;
		while (remaining > 0) {
			int count;
			int bytes_read;

			count = (remaining > TGS_BUFFER_SIZE) ? TGS_BUFFER_SIZE : remaining;
			bytes_read = masked_vbyte_read_loop_delta(read_addr, doc_id_buf, count, last_value);
			read_addr += bytes_read;
			remaining -= count;

//			accumulate_stats_for_term(slice, doc_id_buf, count, desc->non_zero_groups,
//			                          group_stats, slice->shard, desc->grp_buf, desc->metric_buf);
            packed_shard_t* shard = slice->shard;
			prefetch_and_process_array(shard, shard->groups_and_metrics, group_stats, temp_buffer,
			                           doc_id_buf, count, shard->metrics_layout->n_vectors_per_doc,
			                           shard->grp_stat_size);
			last_value = doc_id_buf[count - 1];
		}
	}
	
//	compress_and_send_data(desc, session, group_stats);
	return 0;
}
