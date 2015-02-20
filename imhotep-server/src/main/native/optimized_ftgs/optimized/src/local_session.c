#include <stdlib.h>
#include "imhotep_native.h"
#include "circ_buf.h"

#define CIRC_BUFFER_SIZE						32

int run_tgs_pass(struct worker_desc *worker,
                 struct session_desc *session,
                 union term_union *term,
                 long *addresses,
                 int *docs_per_shard,
                 int *shard_handles,
                 int num_shard,
                 int socket_fd)
{
	struct tgs_desc desc;
	packed_shard_t *shard;

	if (num_shard > session->num_shards) {
		/* error */
		return -1;
	}

	tgs_init(worker, &desc, term, addresses, docs_per_shard, shard_handles, num_shard, socket_fd, session);
	session->current_tgs_pass = &desc;

	int err;
	err = tgs_execute_pass(worker, session, &desc);

	tgs_destroy(&desc);

	session->current_tgs_pass = NULL;

	return err;
}

//This method assumes that the boolean metrics will come first
int create_shard_multicache(struct session_desc *session,
                            	int id,
						uint32_t n_docs,
						int64_t *metric_mins,
						int64_t *metric_maxes,
						int n_metrics)
{
	packed_shard_t *shard;

	shard = &session->shards[id];
	shard->num_docs = n_docs;
	packed_shard_init(shard, n_docs, metric_mins, metric_maxes, n_metrics);

	return id;
}

void session_init(struct session_desc *session,
                  int n_groups,
                  int n_stats,
                  int n_shards)
{
	packed_shard_t *shards;

	session->num_groups = n_groups;
	session->num_stats = n_stats;
	session->num_shards = n_shards;
	session->current_tgs_pass = NULL;

	shards = (packed_shard_t *)calloc(sizeof(packed_shard_t), n_shards);
	for (int i = 0; i < n_shards; i++) {
		shards[i].shard_id = i;
		shards[i].num_docs = -1;
		shards[i].grp_metrics_len = 0;
		shards[i].groups_and_metrics = NULL;
		shards[i].metrics_layout = NULL;
	}
	session->shards = shards;
}

void session_destroy(struct session_desc *session)
{
	packed_shard_t *shards;
	int n_shards;

	shards = session->shards;
	n_shards = session->num_shards;
	for (int i = 0; i < n_shards; i++) {
		packed_shard_destroy(&shards[i]);
	}

	free(shards);
}

#define DEFAULT_BUFFER_SIZE				8192

void worker_init(struct worker_desc *worker, int id, int n_metrics, int num_groups)
{
	worker->id = id;
	worker->buffer_size = DEFAULT_BUFFER_SIZE;
	worker->group_stats_buf = (__m128i *)calloc(sizeof(uint8_t), worker->buffer_size);
		/* allocate and initalize buffers */
	worker->grp_buf = circular_buffer_int_alloc(CIRC_BUFFER_SIZE);
	worker->metric_buf = circular_buffer_vector_alloc((n_metrics+1)/2 * CIRC_BUFFER_SIZE);
	
	worker->bit_tree_buf = calloc(sizeof(struct bit_tree), 1);
	bit_tree_init(worker->bit_tree_buf, num_groups);

}

void worker_destroy(struct worker_desc *worker)
{
	free(worker->group_stats_buf);
	bit_tree_destroy(worker->bit_tree_buf);
	free(worker->bit_tree_buf);
	/* free the intermediate buffers */
	circular_buffer_int_cleanup(worker->grp_buf);
	circular_buffer_vector_cleanup(worker->metric_buf);
}


