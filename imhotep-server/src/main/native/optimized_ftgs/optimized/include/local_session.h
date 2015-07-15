#pragma once

int run_tgs_pass(struct worker_desc *worker,
                 struct session_desc *session,
                 uint8_t term_type,
                 const long int_term,
                 const char *string_term,
                 const int string_term_len,
                 const char **addresses,
                 const int *docs_per_shard,
                 const packed_table_ptr *shards,
                 int num_shard,
                 int socket_num);

int register_shard(struct session_desc *session, packed_table_ptr table);

void session_init(struct session_desc *session,
                  const int n_groups,
                  const int n_stats,
                  const int only_binary_metrics,
                  const packed_table_ptr sample_table);

void session_destroy(struct session_desc *session);

int worker_start_field(struct worker_desc *worker,
                       const char *field_name,
                       int len,
                       int term_type,
                       int socket_num);
int worker_end_field(struct worker_desc *worker, int socket_num);
int worker_end_stream(struct worker_desc *worker, int stream_num);

void worker_init(struct worker_desc *worker,
                 int id,
                 int num_groups,
                 int n_metrics,
                 const int *socket_fds,
                 int num_sockets);
void worker_destroy(struct worker_desc *worker);

