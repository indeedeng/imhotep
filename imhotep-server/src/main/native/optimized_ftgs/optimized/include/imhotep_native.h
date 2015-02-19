#include <emmintrin.h>
#include <smmintrin.h>
#include <tmmintrin.h>
#include <pmmintrin.h>
#include <stdint.h>

struct circular_buffer_vector;

struct worker_desc {
    int id;
    int buffer_size;
    __m128i *group_stats_buf;
};


union term_union {
    uint64_t int_term;
    int string_term_len;
    char *string_term;
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

typedef struct shard_desc {
    int shard_id;
    int num_docs;
    int grp_metrics_len;           // How many __m128 elements do we have in our vector = n_doc_ids*n_vectors_per_doc
    __v16qi *groups_and_metrics;   /* group and metrics data packed into 128b vectors */
    struct packed_metric_desc *metrics_layout;
} packed_shard_t;

struct index_slice_info {
    int n_docs_in_slice;
    uint64_t slice_len;
    uint8_t *slice;
    packed_shard_t *shard;
};

struct tgs_desc {
    int socket_fd;
    union term_union term;
    int n_slices;
    struct index_slice_info *trm_slice_infos;
    __m128i *group_stats;
};

struct session_desc {
    int num_groups;
    int num_stats;
    struct shard_data *shards;
    struct tgs_desc *current_tgs_pass;
};

int tgs_execute_pass(	struct worker_desc *desc,
						struct session_desc *session,
						union term_union *term,
						struct index_slice_info **trm_slice_infos,
						int n_slices);

int tgs_init(struct tgs_desc *info);

void packed_shard_unpack_metrics_into_buffer(packed_shard_t *shard,
									int doc_id,
									struct circular_buffer_vector *buffer);
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

/* return the number of bytes read*/
int slice_copy_range(uint8_t* slice,
                     int *destination,
                     int count_to_read,
                     int *delta_decode_in_out);

