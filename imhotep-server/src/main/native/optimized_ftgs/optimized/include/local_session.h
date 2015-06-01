#pragma once

int run_tgs_pass(struct worker_desc *worker,
                 struct session_desc *session,
                 uint8_t term_type,
                 const long int_term,
                 const char *string_term,
                 const int string_term_len,
                 const long *addresses,
                 const int *docs_per_shard,
                 const packed_table_t **shards,
                 int num_shard,
                 int socket_num);

packed_table_t *create_shard_multicache(uint32_t n_docs,
                                        int64_t *metric_mins,
                                        int64_t *metric_maxes,
                                        int32_t *sizes,
                                        int32_t *vec_nums,
                                        int32_t *offsets_in_vecs,
                                        int8_t *original_idxs,
                                        int n_metrics);
void destroy_shard_multicache(packed_table_t *table);

int register_shard(struct session_desc *session, packed_table_t *table);

void session_init(struct session_desc *session,
                  int n_groups,
                  int n_stats,
                  int only_binary_metrics,
                  packed_table_t *sample_table);

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
                 int *socket_fds,
                 int num_sockets);
void worker_destroy(struct worker_desc *worker);

