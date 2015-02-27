#include "imhotep_native.h"
#include "varintdecode.h"

#define TGS_BUFFER_SIZE						1024


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
			prefetch_and_process_2_arrays(shard,
									shard->groups_and_metrics,
									group_stats,
									desc->metric_buf,
									doc_id_buf,
									count,
									shard->metrics_layout->n_vectors_per_doc,
									shard->grp_stat_size,
									desc->non_zero_groups,
									desc->grp_buf);
			last_value = doc_id_buf[count - 1];
		}
	}
	
//	compress_and_send_data(desc, session, group_stats);
	return 0;
}
