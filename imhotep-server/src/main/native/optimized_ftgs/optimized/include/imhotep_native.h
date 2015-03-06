#pragma once

#include <emmintrin.h>
#include <smmintrin.h>
#include <tmmintrin.h>
#include <pmmintrin.h>
#include <stdint.h>
#include "bit_tree.h"

#define TERM_TYPE_STRING						0
#define TERM_TYPE_INT                           1

#define PREFETCH_DISTANCE                       8

#define ALIGNED_ALLOC(alignment, size) ((alignment) < (size)) ? aligned_alloc(alignment,size) : aligned_alloc(alignment,alignment);

struct circular_buffer_vector;

struct packed_table_desc;
typedef struct packed_table_desc packed_table_t;

struct unpacked_table_desc;
typedef struct unpacked_table_desc unpacked_table_t;

struct bit_fields_and_group {
	uint32_t grp :28;
	uint32_t metrics :4;
};

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
    unpacked_table_t *grp_stats;
    struct circular_buffer_int *grp_buf;
    union term_union **prev_term_by_socket;
    struct buffered_socket *sockets;
};

struct string_term_s {
    int string_term_len;
    char *string_term;
};

union term_union {
    uint64_t int_term;
    struct string_term_s string_term;
};

struct index_slice_info {
    int n_docs_in_slice;
    uint8_t *doc_slice;
    packed_table_t *packed_metrics;
};

struct tgs_desc {
    uint8_t term_type;
    union term_union *term;
    union term_union *previous_term;

    struct index_slice_info *slices;
    int n_slices;

    unpacked_table_t *group_stats;
    unpacked_table_t *temp_table;
    uint32_t temp_table_mask;

    struct circular_buffer_int *grp_buf;
    struct buffered_socket *socket;
};

struct session_desc {
    int num_groups;
    int num_stats;
    uint8_t* stat_order;
    int num_shards;
    packed_table_t **shards;
    unpacked_table_t *temp_buf;
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


/* return the number of bytes read*/
int slice_copy_range(uint8_t* slice,
                     int *destination,
                     int count_to_read,
                     int *delta_decode_in_out);

void socket_init(struct buffered_socket *socket, uint32_t fd);
void socket_destroy(struct buffered_socket *socket);

void lookup_and_accumulate_grp_stats(
                                   packed_table_t *src_table,
                                   unpacked_table_t *dest_table,
                                   uint32_t* row_id_buffer,
                                   int buffer_len,
                                   struct circular_buffer_int *grp_buf,
                                   unpacked_table_t *temp_buf);

packed_table_t *packed_table_create(int n_rows,
                                    int64_t *column_mins,
                                    int64_t *column_maxes,
                                    int n_cols);
void packed_table_destroy(packed_table_t *table);
int packed_table_get_size(packed_table_t *table);
int packed_table_get_row_size(packed_table_t *table);
int packed_table_get_rows(packed_table_t *table);
int packed_table_get_cols(packed_table_t *table);

long packed_table_get_cell(packed_table_t *table, int row, int column);
void packed_table_set_cell(packed_table_t *table, int row, int col, long value);
long packed_table_get_group(packed_table_t *table, int row);
void packed_table_set_group(packed_table_t *table, int row, int value);
void packed_table_set_all_groups(packed_table_t *table, int value);

void packed_table_batch_col_lookup( packed_table_t *table,
                                int * restrict row_ids,
                                int n_row_ids,
                                int64_t * restrict dest,
                                int column);
void packed_table_batch_set_col(   packed_table_t *table,
                            int * restrict row_ids,
                            int n_row_ids,
                            int64_t * restrict col_vals,
                            int col);
void packed_table_set_col_range(packed_table_t *table,
                                const int start_row,
                                const int64_t * restrict col_vals,
                                const int count,
                                const int col);

void packed_table_batch_group_lookup( packed_table_t *table,
                                int * restrict row_ids,
                                int n_row_ids,
                                int32_t * restrict dest);
void packed_table_batch_set_group(   packed_table_t *table,
                            int * restrict row_ids,
                            int n_row_ids,
                            int32_t * restrict group_vals);
void packed_table_set_group_range(packed_table_t *table,
                                  const int start,
                                  const int count,
                                  const int32_t * restrict group_vals);

void packed_table_bit_set_regroup(packed_table_t *table,
                                  const long *bits,
                                  const int target_group,
                                  const int negative_group,
                                  const int positive_group);

void packed_table_unpack_row_to_table(
                                             packed_table_t* src_table,
                                             int src_row_id,
                                             unpacked_table_t* dest_table,
                                             int dest_row_id,
                                             int prefetch_row_id);

unpacked_table_t *unpacked_table_create(packed_table_t *packed_table, int n_rows);
void unpacked_table_destroy(unpacked_table_t *table);
int unpacked_table_get_size(unpacked_table_t *table);
int unpacked_table_get_rows(unpacked_table_t *table);
int unpacked_table_get_cols(unpacked_table_t *table);
unpacked_table_t *unpacked_table_copy_layout(unpacked_table_t *src_table, int num_rows);
long unpacked_table_get_cell(unpacked_table_t *table, int row, int column);
void unpacked_table_set_cell(unpacked_table_t *table, int row, int column, long value);
void unpacked_table_add_rows(unpacked_table_t* src_table,
                                    int src_row_id,
                                    unpacked_table_t* dest_table,
                                    int dest_row_id,
                                    int prefetch_row_id);

