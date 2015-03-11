#include <stdlib.h>
#include <string.h>
#include "imhotep_native.h"
#include "circ_buf.h"
#include "socket.h"

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
                 int socket_fd,
                 struct runtime_err *error)
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

	current_term = calloc(1, sizeof(union term_union));
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
		if (socket->socket_fd == socket_fd) {
		    break;
		}
		curr_socket_idx++;
	} while (curr_socket_idx < worker->num_sockets);
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
  if (err != 0) {
    if (desc.socket->err && error) {
      memcpy(error, desc.socket->err, sizeof(struct runtime_err));
    }
  }

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
packed_table_t *create_shard_multicache(uint32_t n_docs,
                                        int64_t *metric_mins,
                                        int64_t *metric_maxes,
                                        int n_metrics)
{
	return packed_table_create(n_docs, metric_mins, metric_maxes, n_metrics);
}

int register_shard(struct session_desc *session, packed_table_t *table)
{
    for (int i = 0; i < session->num_shards; i++) {
        if (session->shards[i] == NULL) {
            session->shards[i] = table;
            return i;
        }
    }
    return -1;
}

void session_init(struct session_desc *session,
                  int n_groups,
                  int n_stats,
                  uint8_t* stat_order,
                  int n_shards)
{
    packed_table_t **shards;

    session->num_groups = n_groups;
    session->num_stats = n_stats;
    session->stat_order = calloc(n_stats, sizeof(uint8_t));
    memcpy(session->stat_order, stat_order, n_stats);
    session->num_shards = n_shards;
    session->current_tgs_pass = NULL;

    session->temp_buf = NULL;

    shards = (packed_table_t **)calloc(n_shards, sizeof(packed_table_t *));
    session->shards = shards;
}

void session_destroy(struct session_desc *session)
{
    packed_table_t **shards;

    shards = session->shards;
		if (session->temp_buf) unpacked_table_destroy(session->temp_buf);
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
    worker->grp_stats = NULL;
	worker->grp_buf = circular_buffer_int_alloc(CIRC_BUFFER_SIZE);

	worker->num_sockets = num_sockets;
	worker->sockets = calloc(num_sockets, sizeof(struct buffered_socket));
	for (int i = 0; i < num_sockets; i++) {
		socket_init(&worker->sockets[i], socket_fds[i]);
	}

	worker->prev_term_by_socket = calloc(num_sockets, sizeof(union term_union *));
}

void worker_destroy(struct worker_desc *worker)
{
    unpacked_table_destroy(worker->grp_stats);

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
//	free(worker->metric_buf);

	/* free previous term array */
	free(worker->prev_term_by_socket);
}
