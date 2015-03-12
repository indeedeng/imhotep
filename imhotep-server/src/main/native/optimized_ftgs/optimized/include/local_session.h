#pragma once

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
                 struct runtime_err *error);

//This method assumes that the boolean metrics will come first
packed_table_t *create_shard_multicache(uint32_t n_docs,
                                        int64_t *metric_mins,
                                        int64_t *metric_maxes,
                                        int n_metrics);
void destroy_shard_multicache(packed_table_t *table);

int register_shard(struct session_desc *session, packed_table_t *table);

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

