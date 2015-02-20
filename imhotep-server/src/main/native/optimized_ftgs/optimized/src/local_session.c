#include <stdlib.h>
#include "imhotep_native.h"


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

	tgs_init(&desc, term, addresses, docs_per_shard, shard_handles, num_shard, socket_fd);
	session->current_tgs_pass = &desc;

	int err;
	err = tgs_execute_pass(worker, session, &desc);

	tgs_destroy(&desc);

	session->current_tgs_pass = NULL;

	return err;
}
