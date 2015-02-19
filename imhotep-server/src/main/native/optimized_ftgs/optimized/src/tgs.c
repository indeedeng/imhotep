#include "imhotep_native.h"
#include "circ_buf.h"

#define CIRC_BUFFER_SIZE						32
#define TGS_BUFFER_SIZE						1024
#define MAX_BIT_FIELDS						4
#define PREFETCH_DISTANCE					16

struct bit_fields_and_group {
	uint32_t metrics :4;
	uint32_t grp :28;
};

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

/*
 * This should only be called when doc_ids_len is a multiple of PREFETCH_DISTANCE
 */
static void accumulate_stats_for_term(	struct index_slice_info *slice,
								int *doc_ids,
								int doc_ids_len,
								__m128i *group_stats,
								packed_shard_t *shard)
{
	__v16qi* grp_metrics = shard->groups_and_metrics;
	struct packed_metric_desc *packing_desc = shard->metrics_layout;
	struct circular_buffer_int *grp_buf;
	struct circular_buffer_vector *metric_buf;
	int n_vecs_per_doc = packing_desc->n_vectors_per_doc;

	/* allocate and initalize buffers */
	grp_buf = circular_buffer_int_alloc(CIRC_BUFFER_SIZE);
	metric_buf = circular_buffer_vector_alloc(packing_desc->n_metrics * CIRC_BUFFER_SIZE);

	/* prefech the first PREFETCH num grp metrics data */
	for (int32_t i = 0; i < PREFETCH_DISTANCE; i++) {
		__v16qi *prefetch_address;
		int32_t prefetch_doc_id;

		prefetch_doc_id = doc_ids[i];
		prefetch_address = &grp_metrics[prefetch_doc_id * n_vecs_per_doc];
		_mm_prefetch(prefetch_address, _MM_HINT_T0);
	}

	/* process the data */
	for (int32_t i = 0; i < doc_ids_len; i++) {

		uint32_t doc_id = doc_ids[i];
		uint32_t start_idx = doc_id * n_vecs_per_doc;

		/* load group id and unpack metrics */
		struct bit_fields_and_group packed_bf_grp;
		uint32_t bit_fields;
		uint32_t group;

		// TODO: fix me
		/* decode bit fields */
		packed_bf_grp = *((struct bit_fields_and_group *)&grp_metrics[start_idx]);
		bit_fields = packed_bf_grp.metrics;

		/* get group*/
		group = packed_bf_grp.grp;

		/* save group into buffer */
		circular_buffer_int_put(grp_buf, group);

		{
			/* Prefetch group_stats */
			__m128i *prefetch_address;
			prefetch_address = &group_stats[group * n_vecs_per_doc];
			_mm_prefetch(prefetch_address, _MM_HINT_T0);
		}

		/* unpack and save the metrics for this document */
		packed_shard_unpack_metrics_into_buffer(shard, doc_id, metric_buf);
	}    // doc id loop

	/* sum the final buffered stats */
	for (int32_t i = 0; i < PREFETCH_DISTANCE; i++) {
		uint32_t group_id;
		group_id = circular_buffer_int_get(grp_buf);
		for (int32_t j = 0; j < n_vecs_per_doc; j++) {
			__m128i stats = circular_buffer_vector_get(metric_buf);
			group_stats[group_id * n_vecs_per_doc + j] += stats;
		}
	}

	/* free the intermediate buffers */
	circular_buffer_int_cleanup(grp_buf);
	circular_buffer_vector_cleanup(metric_buf);
}


int tgs_init(struct tgs_desc *info)
{
	return 0;
}

int tgs_execute_pass(struct worker_desc *desc,
                     struct session_desc *session,
                     union term_union *term,
                     struct index_slice_info **trm_slice_infos,
                     int n_slices)
{
	int buffer[TGS_BUFFER_SIZE];
	__m128i *group_stats;

	group_stats = allocate_grp_stats(desc, session);
	session->current_tgs_pass->group_stats = group_stats;

	for (int i = 0; i < n_slices; i++) {
		struct index_slice_info *slice;
		int remaining;      /* num docs remaining */
		int delta;          /* delta decode tracker */
		uint8_t *read_addr;

		slice = trm_slice_infos[i];
		remaining = slice->n_docs_in_slice;
		read_addr = slice->slice;
		while (remaining > 0) {
			int count;
			int bytes_read;

			count = (remaining > TGS_BUFFER_SIZE) ? TGS_BUFFER_SIZE : remaining;
			bytes_read = slice_copy_range(read_addr, buffer, count, &delta);
			read_addr += bytes_read;
			remaining -= count;

			accumulate_stats_for_term(slice, buffer, count, group_stats, slice->shard);
		}
	}
	
//	compress_and_send_data(desc, session, group_stats);
	return 0;
}
