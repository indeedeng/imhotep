#include <errno.h>
#include <mmintrin.h>
#include <string.h>
#include <unistd.h>
#include "remote_output.h"

#define TRY(a) { \
    int _err = (a); \
    if (_err != 0) return _err; \
}

#define MIN(a, b) ({ \
    __typeof__(a) _a = (a); \
    __typeof__(b) _b = (b); \
    _a < _b ? _a : _b; \
})

#define MAX(a, b) ({ \
    __typeof__(a) _a = (a); \
    __typeof__(b) _b = (b); \
    _a > _b ? _a : _b; \
})

int flush_buffer(struct buffered_socket* socket)
{
    size_t write_ptr = 0;
    while (write_ptr < socket->buffer_ptr) {
        ssize_t written = write(socket->socket_fd, socket->buffer, socket->buffer_ptr);
        if (written == -1) {
          socket_capture_error(socket, errno);
          return -1;
        }
        write_ptr += written;
    }
    socket->buffer_ptr = 0;
    return 0;
}

static inline int write_byte(struct buffered_socket* socket, const uint8_t value) {
    if (socket->buffer_ptr == socket->buffer_len) {
        TRY(flush_buffer(socket));
    }
    socket->buffer[socket->buffer_ptr] = value;
    socket->buffer_ptr++;
    return 0;
}

static inline int write_bytes(struct buffered_socket* socket,
                              const uint8_t* restrict bytes,
                              const size_t len) {
    size_t write_ptr = 0;
    while (write_ptr < len) {
        if (socket->buffer_ptr == socket->buffer_len) {
            TRY(flush_buffer(socket));
        }
        size_t copy_len = MIN(len - write_ptr, socket->buffer_len - socket->buffer_ptr);
        memcpy(socket->buffer + socket->buffer_ptr, bytes + write_ptr, copy_len);
        write_ptr += copy_len;
        socket->buffer_ptr += copy_len;
    }
    return 0;
}

static int write_vint64(struct buffered_socket* socket, const uint64_t i) {
    if (i < 1L << 7) {
        TRY(write_byte(socket, (uint8_t) i));
    } else if (i < 1L << 14) {
        TRY(write_byte(socket, (uint8_t) ((i&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (i>>7)));
    } else if (i < 1L << 21) {
        TRY(write_byte(socket, (uint8_t) ((i&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>7)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (i>>14)));
    } else if (i < 1L << 28) {
        TRY(write_byte(socket, (uint8_t) ((i&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>7)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>14)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (i>>21)));
    } else if (i < 1L << 35) {
        TRY(write_byte(socket, (uint8_t) ((i&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>7)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>14)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>21)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (i>>28)));
    } else if (i < 1L << 42) {
        TRY(write_byte(socket, (uint8_t) ((i&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>7)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>14)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>21)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>28)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (i>>35)));
    } else if (i < 1L << 49) {
        TRY(write_byte(socket, (uint8_t) ((i&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>7)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>14)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>21)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>28)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>35)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (i>>42)));
    } else if (i < 1L << 56) {
        TRY(write_byte(socket, (uint8_t) ((i&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>7)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>14)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>21)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>28)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>35)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>42)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (i>>49)));
    } else if (i < 1L << 63) {
        TRY(write_byte(socket, (uint8_t) ((i&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>7)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>14)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>21)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>28)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>35)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>42)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>49)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (i>>56)));
    } else {
        TRY(write_byte(socket, (uint8_t) ((i&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>7)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>14)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>21)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>28)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>35)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>42)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>49)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (((i>>56)&0x7F) | 0x80)));
        TRY(write_byte(socket, (uint8_t) (i>>63)));
    }
    return 0;
}

static inline int write_svint64(struct buffered_socket* socket, const int64_t i) {
    return write_vint64(socket, (i << 1) ^ (i >> 63));
}

int write_field_start(struct ftgs_outstream* stream,
                const char *field_name,
                const int len,
                const int term_type)
{
	struct buffered_socket* socket = &stream->socket;

    /* write 1 for int field, 2 for string field */
    switch (term_type) {
		case TERM_TYPE_INT:
			TRY(write_byte(socket, 1));
			break;
		case TERM_TYPE_STRING:
			TRY(write_byte(socket, 2));
			break;
    }

    /* write new field len (varint encoded) */
    TRY(write_vint64(socket, len));
    // write new field name
    TRY(write_bytes(socket, (uint8_t *)field_name, len));

    /* reset prev_term (-1 for int, "" for string) */
    term_reset(&stream->prev_term);

    /* set prev_term type to new field type */
    stream->term_type = term_type;

    return 0;
}

int write_field_end(struct ftgs_outstream* stream)
{
	struct buffered_socket* socket = &stream->socket;

    /* write 0 */
    TRY(write_byte(socket, 0));

    /* if field was a string field, write another 0 */
    if (stream->term_type == TERM_TYPE_STRING) {
        TRY(write_byte(socket, 0));
    }

	/* flush whatever is left in the socket buffer */
    TRY(flush_buffer(&stream->socket));

    return 0;
}

int write_stream_end(struct ftgs_outstream* stream)
{
    struct buffered_socket* socket = &stream->socket;

    /* write 0 */
    TRY(write_byte(socket, 0));

    /* flush whatever is left in the socket buffer */
    TRY(flush_buffer(&stream->socket));

    return 0;
}

static int write_group_stats(struct buffered_socket* socket,
                             const uint32_t* restrict non_zero_groups,
                             const int nz_group_count,
                             const unpacked_table_t* restrict group_stats,
                             const int num_stats) {
    int32_t previous_group = -1;
    int i = 0;
    for (; i < nz_group_count - PREFETCH_DISTANCE; i++) {
        const uint32_t group = non_zero_groups[i];
        int stat_index = 0;
        TRY(write_vint64(socket, group - previous_group));
        previous_group = group;
        const uint32_t prefetch_group = non_zero_groups[i+PREFETCH_DISTANCE];
        const int64_t* prefetch_start = unpacked_table_get_rows_addr(group_stats, prefetch_group);

        for (; stat_index <= num_stats-8; stat_index += 8) {
            int64_t stat;
            stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index+0);
            TRY(write_svint64(socket, stat));
            stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index+1);
            TRY(write_svint64(socket, stat));
            stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index+2);
            TRY(write_svint64(socket, stat));
            stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index+3);
            TRY(write_svint64(socket, stat));
            stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index+4);
            TRY(write_svint64(socket, stat));
            stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index+5);
            TRY(write_svint64(socket, stat));
            stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index+6);
            TRY(write_svint64(socket, stat));
            stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index+7);
            TRY(write_svint64(socket, stat));

            _mm_prefetch(prefetch_start+stat_index, _MM_HINT_T0);
        }
        if (stat_index < num_stats) {
            do {
                int64_t stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index);
                TRY(write_svint64(socket, stat));
                stat_index++;
            } while (stat_index < num_stats);

            const int64_t* prefetch_address = prefetch_start + stat_index;
            _mm_prefetch(prefetch_address, _MM_HINT_T0);
        }
    }

    /* final Prefetch_distance value */
    for (; i < nz_group_count; i++) {
        const uint32_t group = non_zero_groups[i];
        int stat_index = 0;
        TRY(write_vint64(socket, group - previous_group));
        previous_group = group;

        for (; stat_index <= num_stats-8; stat_index += 8) {
            int64_t stat;
            stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index+0);
            TRY(write_svint64(socket, stat));
            stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index+1);
            TRY(write_svint64(socket, stat));
            stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index+2);
            TRY(write_svint64(socket, stat));
            stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index+3);
            TRY(write_svint64(socket, stat));
            stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index+4);
            TRY(write_svint64(socket, stat));
            stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index+5);
            TRY(write_svint64(socket, stat));
            stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index+6);
            TRY(write_svint64(socket, stat));
            stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index+7);
            TRY(write_svint64(socket, stat));
        }
        if (stat_index < num_stats) {
            do {
                int64_t stat = unpacked_table_get_and_clear_remapped_cell(group_stats, group, stat_index);
                TRY(write_svint64(socket, stat));
                stat_index++;
            } while (stat_index < num_stats);
        }
    }
    TRY(write_byte(socket, 0));
    return 0;
}

static inline size_t prefix_len(const struct string_term_s* restrict term,
                                const struct string_term_s* restrict previous_term) {
    size_t min = MIN(term->len, previous_term->len);
    for (size_t i = 0; i < min; i++) {
        if (term->term[i] != previous_term->term[i]) {
        	return i;
        }
    }
    return min;
}

int write_term_group_stats(const struct session_desc* session,
                           struct tgs_desc* tgs,
                           const uint32_t* restrict groups,
                           const int term_group_count)
{
	struct buffered_socket *socket = &tgs->stream->socket;
	struct term_s *prev_term = &tgs->stream->prev_term;

    /* Short-circuit for invalid socket fds so that we can
       deliberately skip this code path in testing contexts e.g
       test_tgs. */
    if (socket->socket_fd < 0) return 0;

    if (tgs->term_type == TERM_TYPE_INT) {
        if (prev_term->int_term == -1 && tgs->term.int_term == -1) {
            TRY(write_byte(socket, 0x80));
            TRY(write_byte(socket, 0));
        } else {
            TRY(write_vint64(socket, tgs->term.int_term - prev_term->int_term));
        }
        term_update_int(prev_term, &tgs->term);
    } else {
        struct string_term_s* term = &tgs->term.string_term;
        struct string_term_s* previous_term = &prev_term->string_term;
        size_t p_len = prefix_len(term, previous_term);
        TRY(write_vint64(socket, previous_term->len - p_len + 1));
        TRY(write_vint64(socket, term->len - p_len));
        TRY(write_bytes(socket, (uint8_t*)(term->term + p_len), term->len - p_len));
        term_update_string(prev_term, &tgs->term);
    }
    int64_t term_doc_freq = 0;
    for (int i = 0; i < tgs->n_slices; i++) {
        term_doc_freq += tgs->slices[i].n_docs_in_slice;
    }
    TRY(write_svint64(socket, term_doc_freq));
    int num_stats = session->num_stats;
    TRY(write_group_stats(socket, groups, term_group_count, tgs->group_stats, num_stats));

    return 0;
}
