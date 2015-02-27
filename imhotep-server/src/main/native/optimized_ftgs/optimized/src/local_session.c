#include <stdlib.h>
#include <string.h>
#include "imhotep_native.h"
#include "circ_buf.h"

#define CIRC_BUFFER_SIZE						32

int run_tgs_pass(struct worker_desc *worker,
                 struct session_desc *session,
                 uint8_t term_type,
                 int int_term,
                 char *string_term,
                 long *addresses,
                 int *docs_per_shard,
                 int *shard_handles,
                 int num_shard,
                 int socket_fd)
{
	struct tgs_desc desc;
	struct buffered_socket *socket;
	union term_union* current_term;
	union term_union* previous_term;
	int curr_socket_idx;

	/* just in case. Kinda unnecessary to check this here */
	if (num_shard > session->num_shards) {
		/* error */
		return -1;
	}

	current_term = calloc(sizeof(union term_union), 1);
	switch(term_type) {
		case TERM_TYPE_STRING:
		/* copy string into term */
		break;
		case TERM_TYPE_INT:
		current_term->int_term = int_term;
		break;
	}

	/* find the socket data struct by its file descriptor */
	curr_socket_idx = 0;
	do {
		socket = &worker->sockets[curr_socket_idx];
		curr_socket_idx++;
	} while (socket->socket_fd != socket_fd && curr_socket_idx < worker->num_sockets);
	if (curr_socket_idx >= worker->num_sockets) {
		/* error */
		return -1;
	}

	/* find previous term */
	previous_term = worker->prev_term_by_socket[curr_socket_idx];

	/* init the tsg struct */
	tgs_init(worker, 
	         &desc, 
	         term_type, 
	         current_term, 
	         previous_term, 
	         addresses, 
	         docs_per_shard, 
	         shard_handles, 
	         num_shard, 
	         socket, 
	         session);
	         
	/* save just in case ... something? */
	session->current_tgs_pass = &desc;

	/* do the Term Group Stats accumulation pass */
	int err;
	err = tgs_execute_pass(worker, session, &desc);

	/* clean up the tgs structure */
	tgs_destroy(&desc);
	
	/* save the current term as previous, free previous term */
	worker->prev_term_by_socket[curr_socket_idx] = current_term;
	if (previous_term != NULL) {
		free(previous_term);
	}

	session->current_tgs_pass = NULL;

	return err;
}

//This method assumes that the boolean metrics will come first
void *create_shard_multicache(uint32_t n_docs,
                              int64_t *metric_mins,
                              int64_t *metric_maxes,
                              int n_metrics)
{
	packed_shard_t *shard;

    shard = calloc(sizeof(packed_shard_t), 1);
	packed_shard_init(shard, n_docs, metric_mins, metric_maxes, n_metrics);
	return shard;
}

void session_init(struct session_desc *session,
                  int n_groups,
                  int n_stats,
                  uint8_t* stat_order,
                  int n_shards)
{
	packed_shard_t *shards;

	session->num_groups = n_groups;
	session->num_stats = n_stats;
	session->stat_order = calloc(sizeof(uint8_t), n_stats);
	session->num_shards = n_shards;
	session->current_tgs_pass = NULL;
	
	memcpy(session->stat_order, stat_order, n_stats);

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
	free(session->stat_order);
	free(shards);
}

#define DEFAULT_BUFFER_SIZE				8192

void worker_init(struct worker_desc *worker, 
                 int id, 
                 int num_groups,
                 int n_metrics, 
                 int *socket_fds, 
                 int num_sockets)
{
	worker->id = id;
	worker->buffer_size = DEFAULT_BUFFER_SIZE;
	worker->group_stats_buf = (__m128i *)calloc(sizeof(uint8_t), worker->buffer_size);
		/* allocate and initalize buffers */
	worker->grp_buf = circular_buffer_int_alloc(CIRC_BUFFER_SIZE);
	worker->metric_buf = circular_buffer_vector_alloc((n_metrics+1)/2 * CIRC_BUFFER_SIZE);
//    worker->metric_buf = aligned_alloc(64, sizeof(uint64_t) * n_metrics * 2);
	
	worker->bit_tree_buf = calloc(sizeof(struct bit_tree), 1);
	bit_tree_init(worker->bit_tree_buf, num_groups);
	
	worker->num_sockets = num_sockets;
	worker->sockets = calloc(sizeof(struct buffered_socket), num_sockets);
	for (int i = 0; i < num_sockets; i++) {
		socket_init(&worker->sockets[i], socket_fds[i]);
	}
	
	worker->prev_term_by_socket = calloc(sizeof(union term_union *), num_sockets);
}

void worker_destroy(struct worker_desc *worker)
{
	free(worker->group_stats_buf);
	bit_tree_destroy(worker->bit_tree_buf);
	free(worker->bit_tree_buf);
	
	/* free socket and term entries */
	for (int i = 0; i < worker->num_sockets; i++) {
		socket_destroy(&worker->sockets[i]);
		
		/* free previous term tracking entries */
		union term_union *t = worker->prev_term_by_socket[i];
		if (t != NULL) {
			free(t);
		}
	}
	
	/* free socket array */
	free(worker->sockets);
	
	/* free the intermediate buffers */
	circular_buffer_int_cleanup(worker->grp_buf);
	circular_buffer_vector_cleanup(worker->metric_buf);
//	free(worker->metric_buf);
	
	/* free previous term array */
	free(worker->prev_term_by_socket);
}


