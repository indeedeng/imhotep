#pragma once

#include <emmintrin.h>
#include <smmintrin.h>
#include <tmmintrin.h>
#include <pmmintrin.h>
#include <stdint.h>
#include "bit_tree.h"

#define TERM_TYPE_STRING						0
#define TERM_TYPE_INT                           1

struct circular_buffer_vector;

struct buffered_socket {
    int socket_fd;
    uint8_t* buffer;
    size_t buffer_ptr;
    size_t buffer_len;
};

struct worker_desc {
    int id;
    int buffer_size;
    int num_sockets;
    __m128i *group_stats_buf;
    struct buffered_socket *sockets;
    struct bit_tree *bit_tree_buf;
    struct circular_buffer_int *grp_buf;
	struct circular_buffer_vector *metric_buf;
	union term_union **prev_term_by_socket;
};

struct string_term_s {
    int string_term_len;
    char *string_term;
};

union term_union {
    uint64_t int_term;
    struct string_term_s string_term;
};

struct packed_metric_desc {
    uint8_t n_boolean_metrics;      // Number of boolean metrics
    int n_metrics;                  // Total number of metrics
    uint8_t *index_metrics;         // Where in the vector is each metric, counting booleans
    uint8_t *metric_n_vector;       // The vector in which the metric is on
    int64_t *metric_mins;           // The minimal value of each metric

    __v16qi *shuffle_vecs_get1;     // shuffle vectors to get metrics, 1 at a time, *NOT* counting booleans
    __v16qi *shuffle_vecs_put;      // shuffle vectors to put metrics, 1 at a time, *NOT* counting booleans
    __v16qi *blend_vecs_put;        // blend vectors to blend metrics, 1 at a time, *NOT* counting booleans

    __v16qi *shuffle_vecs_get2;     // shuffle vectors to get metrics, 2 at a time, *NOT* counting booleans

    int n_vectors_per_doc;          // How many __m128 vectors does a single doc uses
    uint8_t *n_metrics_per_vector;  // The amount of metrics in each of the vectors, *NOT* counting booleans
    
    uint8_t n_metrics_aux_index;    // used to control for how many metrics we have already generated the index
};

typedef struct shard_data {
    int shard_id;
    int num_docs;
    int grp_metrics_len;           // How many __m128 elements do we have in our vector = n_doc_ids*n_vectors_per_doc
    __v16qi *groups_and_metrics;   /* group and metrics data packed into 128b vectors */
    int n_stat_vecs_per_grp;       // (n_metrics+1)/2
    int grp_stat_size;             // in units of 16 bytes. stat sums for a group are padded to align to cache lines, this must be 1 or even
    struct packed_metric_desc *metrics_layout;
} packed_shard_t;

struct index_slice_info {
    int n_docs_in_slice;
    uint8_t *slice;
    packed_shard_t *shard;
};

struct tgs_desc {
    struct buffered_socket *socket;
    int n_slices;
    uint8_t term_type;
    union term_union *term;
    union term_union *previous_term;
    struct index_slice_info *trm_slice_infos;
    struct bit_tree *non_zero_groups;
    __m128i *group_stats;
    struct circular_buffer_int *grp_buf;
	struct circular_buffer_vector *metric_buf;
};

struct session_desc {
    int num_groups;
    int num_stats;
    uint8_t* stat_order;
    int num_shards;
    packed_shard_t *shards;
    struct tgs_desc *current_tgs_pass;
};

int tgs_execute_pass(struct worker_desc *worker,
                     struct session_desc *session,
                     struct tgs_desc *desc);

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
              struct session_desc *session);
void tgs_destroy(struct tgs_desc *desc);


void packed_shard_unpack_metrics_into_buffer(packed_shard_t *shard,
									int doc_id,
									struct circular_buffer_vector *buffer,
									int prefetch_doc_id);
void packed_shard_lookup_groups(	packed_shard_t *shard,
							int * restrict doc_ids,
							int n_doc_ids,
							int64_t * restrict groups);
void packed_shard_update_groups(	packed_shard_t *shard,
							int * restrict doc_ids,
							int n_doc_ids,
							int64_t * restrict groups);
void packed_shard_lookup_metric_values(	packed_shard_t *shard,
								int * restrict doc_ids,
								int n_doc_ids,
								int64_t * restrict dest,
								int metric_index);
void packed_shard_update_metric(	packed_shard_t *shard,
							int * restrict doc_ids,
							int n_doc_ids,
							int64_t * restrict metric_vals,
							int metric_index);
void packed_shard_init(	packed_shard_t *shard,
					uint32_t n_docs,
					int64_t *metric_mins,
					int64_t *metric_maxes,
					int n_metrics);
void packed_shard_destroy(packed_shard_t *shard);

void dump_shard(packed_shard_t *shard);

/* return the number of bytes read*/
int slice_copy_range(uint8_t* slice,
                     int *destination,
                     int count_to_read,
                     int *delta_decode_in_out);

void socket_init(struct buffered_socket *socket, uint32_t fd);
void socket_destroy(struct buffered_socket *socket);
