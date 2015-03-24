#pragma once

int run_tgs_pass(struct worker_desc *worker,
                 struct session_desc *session,
                 uint8_t term_type,
                 int int_term,
                 char *string_term,
                 int string_term_len,
                 long *addresses,
                 int *docs_per_shard,
                 int num_shard,
                 int split_idx);

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
                  packed_table_t **shards,
                  int n_shards);

void session_destroy(struct session_desc *session);

int worker_start_field(struct worker_desc *worker,
                       char *field_name,
                       int len,
                       int term_type);
int worker_end_field(struct worker_desc *worker);

void worker_init(struct worker_desc *worker,
                 int id,
                 int num_groups,
                 int n_metrics,
                 int *socket_fds,
                 int num_sockets);
void worker_destroy(struct worker_desc *worker);

