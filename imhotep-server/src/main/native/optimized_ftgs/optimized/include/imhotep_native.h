#pragma once

#include <emmintrin.h>
#include <smmintrin.h>
#include <tmmintrin.h>
#include <pmmintrin.h>
#include <malloc.h>
#include <stdlib.h>
#include <stdint.h>
#include "bit_tree.h"
#include <malloc.h>

#define TERM_TYPE_STRING 0
#define TERM_TYPE_INT    1

#define PREFETCH_DISTANCE 8

#define SIZE_OF_ERRSTR 256

#ifdef _SANITIZE_
#define ALIGNED_ALLOC(alignment, size) malloc(size);
#else
#define ALIGNED_ALLOC(alignment, size) (((alignment) < (size)) ? memalign(alignment,size) : memalign(alignment,alignment));
#endif

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
    int64_t int_term;
    struct string_term_s string_term;
};

struct ftgs_outstream {
	struct buffered_socket socket;
	int term_type;
	struct term_s prev_term;
};

struct worker_desc {
    int num_streams;
    struct ftgs_outstream *out_streams;
    struct runtime_err error;
//    long timings[32];
};

struct index_slice_info {
    int n_docs_in_slice;
    uint8_t *doc_slice;
    packed_table_t *packed_metrics;
};

struct tgs_desc {
    uint8_t term_type;
    struct term_s term;

    int n_slices;

    unpacked_table_t *group_stats;
    unpacked_table_t *temp_table;
    uint32_t temp_table_mask;
    uint32_t *updated_groups;

    struct circular_buffer_int *grp_buf;
    struct ftgs_outstream *stream;
    struct index_slice_info slices[1024];
};

struct session_desc {
    int num_groups;
    int num_stats;
    int num_shards;
    int only_binary_metrics;
    unpacked_table_t *temp_buf;
    unpacked_table_t *grp_stats;
    struct circular_buffer_int *grp_buf;
    uint32_t *nz_grps_buf;
};

void term_destroy(uint8_t term_type, struct term_s *term);
int tgs_execute_pass(struct worker_desc *worker,
                     const struct session_desc *session,
                     struct tgs_desc *desc);

void tgs_init(struct worker_desc *worker,
              struct tgs_desc *desc,
              uint8_t term_type,
              long *addresses,
              int *docs_per_shard,
              packed_table_t **shards,
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
void term_init(
               struct term_s *term,
               uint8_t term_type,
               long int_term,
               char *string_term,
               int string_term_len);
void term_destroy(uint8_t term_type, struct term_s *term);
void term_update_int(struct term_s *term, struct term_s *new_term);
void term_update_string(struct term_s *term, struct term_s *new_term);
void term_reset(struct term_s *term);

void lookup_and_accumulate_grp_stats(
                                     const struct worker_desc *worker,
                                     const packed_table_t *src_table,
                                     const unpacked_table_t *dest_table,
                                     const uint32_t* row_id_buffer,
                                     const int buffer_len,
                                     const struct circular_buffer_int *grp_buf,
                                     const unpacked_table_t *temp_buf);

void binary_lookup_and_accumulate_grp_stats(const struct worker_desc *worker,
                                            const packed_table_t *src_table,
                                            const unpacked_table_t *dest_table,
                                            const uint32_t* restrict row_id_buffer,
                                            const int buffer_len,
                                            const struct circular_buffer_int *grp_buf);

packed_table_t *packed_table_create(int n_rows,
                                    int64_t *column_mins,
                                    int64_t *column_maxes,
                                    int32_t *sizes,
                                    int32_t *vec_nums,
                                    int32_t *offsets_in_vecs,
                                    int8_t *original_idx,
                                    int n_cols);
void packed_table_destroy(packed_table_t *table);

/*
 * Attribute Getters
 */
int packed_table_is_binary_only(const packed_table_t *table);
int packed_table_get_size(const packed_table_t *table);
int packed_table_get_row_size(const packed_table_t *table);
int packed_table_get_rows(const packed_table_t *table);
int packed_table_get_cols(const packed_table_t *table);
__v16qi * packed_table_get_row_addr(const packed_table_t *table, int row);

/*
 * External cell accessors and getters
 */
long packed_table_get_cell(const packed_table_t *table, const int row, const int column);
void packed_table_set_cell(const packed_table_t *table,
                           const int row,
                           const int col,
                           const long value);
int packed_table_get_num_groups(const packed_table_t *table);
int packed_table_get_group(const packed_table_t *table, const int row);
void packed_table_set_group(const packed_table_t *table, const int row, const int value);
void packed_table_set_all_groups(const packed_table_t *table, const int value);

void packed_table_batch_col_lookup(
        const packed_table_t *table,
        const int * restrict row_ids,
        const int n_row_ids,
        int64_t * restrict dest,
        const int column);

void packed_table_batch_set_col(
        const packed_table_t *table,
        const int * restrict row_ids,
        const int n_row_ids,
        const int64_t * restrict col_vals,
        const int col);

void packed_table_set_col_range(
        const packed_table_t *table,
        const int start_row,
        const int64_t * restrict col_vals,
        const int count,
        int col);

void packed_table_batch_group_lookup(
        const packed_table_t *table,
        const int * restrict row_ids,
        const int n_row_ids,
        int32_t * restrict dest);

void packed_table_batch_set_group(
        const packed_table_t *table,
        const int * restrict row_ids,
        const int n_row_ids,
        const int32_t * restrict group_vals);

void packed_table_set_group_range(
        const packed_table_t *table,
        const int start,
        const int count,
        const int32_t * restrict group_vals);

void packed_table_bit_set_regroup(
        const packed_table_t *table,
        const long* restrict bits,
        const int target_group,
        const int negative_group,
        const int positive_group);

void packed_table_prefetch_row(const packed_table_t *table, const int row_id);

/*
 *  FTGS below:
 */
void packed_table_unpack_row_to_table(const packed_table_t* src_table,
                                      const int src_row_id,
                                      const unpacked_table_t* dest_table,
                                      const int dest_row_id,
                                      const int prefetch_row_id);

unpacked_table_t *unpacked_table_create(packed_table_t *packed_table, int n_rows);
void unpacked_table_destroy(unpacked_table_t *table);
int unpacked_table_get_size(const unpacked_table_t *table);
int unpacked_table_get_rows(const unpacked_table_t *table);
int unpacked_table_get_cols(const unpacked_table_t *table);
unpacked_table_t *unpacked_table_copy_layout(const unpacked_table_t *src_table, const int num_rows);
struct bit_tree* unpacked_table_get_non_zero_rows(const unpacked_table_t* table);

int remap_docs_in_target_groups_int8_t(packed_table_t* packed_table,
                                       int8_t*         results,
                                       uint8_t*        doc_id_stream,
                                       size_t          n_doc_ids,
                                       int*            remappings,
                                       long            placeholder_group);

int remap_docs_in_target_groups_uint16_t(packed_table_t* packed_table,
                                        uint16_t*       results,
                                        uint8_t*        doc_id_stream,
                                        size_t          n_doc_ids,
                                        int*            remappings,
                                        long            placeholder_group);

int remap_docs_in_target_groups_int16_t(packed_table_t* packed_table,
                                        int16_t*       results,
                                        uint8_t*        doc_id_stream,
                                        size_t          n_doc_ids,
                                        int*            remappings,
                                        long            placeholder_group);

int remap_docs_in_target_groups_int32_t(packed_table_t* packed_table,
                                        int32_t*        results,
                                        uint8_t*        doc_id_stream,
                                        size_t          n_doc_ids,
                                        int*            remappings,
                                        long            placeholder_group);

long unpacked_table_get_cell(
        const unpacked_table_t * restrict table,
        const int row,
        const int column);
void unpacked_table_set_cell(
        const unpacked_table_t * restrict table,
        const int row,
        const int column,
        const long value);
void *unpacked_table_get_rows_addr(const unpacked_table_t * restrict table, const int row);
void unpacked_table_clear(const unpacked_table_t *table);
int64_t unpacked_table_get_and_clear_remapped_cell(
        const unpacked_table_t *table,
        const int row,
        const int orig_idx);
void unpacked_table_add_rows(
        const unpacked_table_t* restrict src_table,
        const int src_row_id,
        const unpacked_table_t* restrict dest_table,
        const int dest_row_id,
        const int prefetch_row_id);
