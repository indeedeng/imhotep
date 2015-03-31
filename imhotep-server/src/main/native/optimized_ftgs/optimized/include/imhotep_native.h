#pragma once

#include <emmintrin.h>
#include <smmintrin.h>
#include <tmmintrin.h>
#include <pmmintrin.h>
#include <malloc.h>
#include <stdlib.h>
#include <stdint.h>
#include "bit_tree.h"


#define TERM_TYPE_STRING 0
#define TERM_TYPE_INT    1

#define PREFETCH_DISTANCE 8

#define SIZE_OF_ERRSTR 256

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

struct string_term_s {
    char *term;
    int len;
};

struct runtime_err {
    int  code;
    char str[SIZE_OF_ERRSTR];
};

struct buffered_socket {
    uint8_t* buffer;
    size_t buffer_ptr;
    size_t buffer_len;
    struct runtime_err* err;
    int socket_fd;
};

struct term_s {
    uint64_t int_term;
    struct string_term_s string_term;
};

struct ftgs_outstream {
	struct buffered_socket socket;
	int term_type;
	struct term_s prev_term;
};

struct worker_desc {
    int id;
    int buffer_size;
    int num_streams;
    unpacked_table_t *grp_stats;
    struct circular_buffer_int *grp_buf;
    struct ftgs_outstream *out_streams;
    struct runtime_err error;
};

struct index_slice_info {
    int n_docs_in_slice;
    uint8_t *doc_slice;
    packed_table_t *packed_metrics;
};

struct tgs_desc {
    uint8_t term_type;
    struct term_s *term;

    struct index_slice_info *slices;
    int n_slices;

    unpacked_table_t *group_stats;
    unpacked_table_t *temp_table;
    uint32_t temp_table_mask;

    struct circular_buffer_int *grp_buf;
    struct ftgs_outstream *stream;
};

struct session_desc {
    int num_groups;
    int num_stats;
    int num_shards;
    packed_table_t **shards;
    unpacked_table_t *temp_buf;
    struct tgs_desc *current_tgs_pass;
};

void term_destroy(uint8_t term_type, struct term_s *term);
int tgs_execute_pass(struct worker_desc *worker,
                     struct session_desc *session,
                     struct tgs_desc *desc);

void tgs_init(struct worker_desc *worker,
              struct tgs_desc *desc,
              uint8_t term_type,
              struct term_s *term,
              long *addresses,
              int *docs_per_shard,
              int num_shard,
              struct ftgs_outstream *stream,
              struct session_desc *session);
void tgs_destroy(struct tgs_desc *desc);


/* return the number of bytes read*/
int slice_copy_range(uint8_t* slice,
                     int *destination,
                     int count_to_read,
                     int *delta_decode_in_out);

void stream_init(struct ftgs_outstream *stream, uint32_t fd);
void stream_destroy(struct ftgs_outstream *stream);
void socket_capture_error(struct buffered_socket *socket, const int code);
struct term_s *term_create(uint8_t term_type,
                              int int_term,
                              char *string_term,
                              int string_term_len);
void term_destroy(uint8_t term_type, struct term_s *term);
void term_update_int(struct term_s *term, struct term_s *new_term);
void term_update_string(struct term_s *term, struct term_s *new_term);
void term_reset(struct term_s *term);

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
                                    int32_t *sizes,
                                    int32_t *vec_nums,
                                    int32_t *offsets_in_vecs,
                                    int8_t *original_idx,
                                    int n_cols);
void packed_table_destroy(packed_table_t *table);
int packed_table_get_size(packed_table_t *table);
int packed_table_get_row_size(packed_table_t *table);
int packed_table_get_rows(packed_table_t *table);
int packed_table_get_cols(packed_table_t *table);
__v16qi * packed_table_get_row_addr(packed_table_t *table, int row);

long packed_table_get_cell(packed_table_t *table, int row, int column);
void packed_table_set_cell(packed_table_t *table, int row, int col, long value);
int packed_table_get_num_groups(packed_table_t *table);
int packed_table_get_group(packed_table_t *table, int row);
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
struct bit_tree* unpacked_table_get_non_zero_rows(unpacked_table_t* table);

int remap_docs_in_target_groups(packed_table_t* packed_table,
                                int*            results,
                                uint8_t*        doc_id_stream,
                                size_t          n_doc_ids,
                                int*            remappings,
                                long            placeholder_group);

long unpacked_table_get_cell(const unpacked_table_t * restrict table,
                             const int row,
                             const int column);
void unpacked_table_set_cell(const unpacked_table_t * restrict table,
                             const int row,
                             const int column,
                             const long value);
void *unpacked_table_get_rows_addr(const unpacked_table_t * restrict table, const int row);
int64_t unpacked_table_get_remapped_cell(const unpacked_table_t *table,
                                         const int row,
                                         const int orig_idx);
void unpacked_table_add_rows(const unpacked_table_t* restrict src_table,
                             const int src_row_id,
                             unpacked_table_t* restrict dest_table,
                             const int dest_row_id,
                             const int prefetch_row_id);
