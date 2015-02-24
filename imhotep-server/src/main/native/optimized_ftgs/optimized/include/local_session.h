
int run_tgs_pass(struct worker_desc *worker,
                 struct session_desc *session,
                 uint8_t term_type,
                 int int_term,
                 char *string_term,
                 long *addresses,
                 int *docs_per_shard,
                 int *shard_handles,
                 int num_shard,
                 int socket_fd);
//This method assumes that the boolean metrics will come first
int create_shard_multicache(struct session_desc *session,
                            	int id,
						uint32_t n_docs,
						int64_t *metric_mins,
						int64_t *metric_maxes,
						int n_metrics);

void session_init(struct session_desc *session,
                  int n_groups,
                  int n_stats,
                  uint8_t* stat_order,
                  int n_shards);

void session_destroy(struct session_desc *session);

void worker_init(struct worker_desc *worker,
                 int id,
                 int num_groups,
                 int n_metrics,
                 int *socket_fds,
                 int num_sockets);
void worker_destroy(struct worker_desc *worker);

