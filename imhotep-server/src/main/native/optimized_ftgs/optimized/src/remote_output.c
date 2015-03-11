#include <errno.h>
#include <mmintrin.h>
#include <string.h>
#include <unistd.h>
#include "remote_output.h"

//#define PREFETCH_DISTANCE 16
//
// TODO: throw an exception with JNI

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

static int flush_buffer(struct buffered_socket* socket) {
    size_t write_ptr = 0;
    while (write_ptr < socket->buffer_ptr) {
        ssize_t written = write(socket->socket_fd, socket->buffer, socket->buffer_len);
        if (written == -1) {
          socket_capture_error(socket, errno);
          return -1;
        }
        write_ptr += written;
    }
    socket->buffer_ptr = 0;
    return 0;
}

static int write_byte(struct buffered_socket* socket, uint8_t value) {
    if (socket->buffer_ptr == socket->buffer_len) {
        TRY(flush_buffer(socket));
    }
    socket->buffer[socket->buffer_ptr] = value;
    socket->buffer_ptr++;
    return 0;
}

static int write_bytes(struct buffered_socket* socket, uint8_t* bytes, size_t len) {
    size_t write_ptr = 0;
    while (write_ptr < len) {
        if (socket->buffer_ptr == socket->buffer_len) {
            TRY(flush_buffer(socket));
        }
        size_t copy_len = MIN(len - write_ptr, socket->buffer_len - socket->buffer_ptr);
        memcpy(socket->buffer + socket->buffer_ptr, bytes + write_ptr, copy_len);
        write_ptr += copy_len;
    }
    return 0;
}

static int write_vint64(struct buffered_socket* socket, uint64_t i) {
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

static int write_svint64(struct buffered_socket* socket, int64_t i) {
    return write_vint64(socket, (i << 1) ^ (i >> 63));
}

int write_group_stats(struct buffered_socket* socket, uint32_t* groups, size_t term_group_count,
             int64_t* group_stats, int num_stats, size_t stats_size, uint8_t* stat_order) {
    int32_t previous_group = -1;
    for (size_t i = 0; i < term_group_count; i++) {
        uint32_t group = groups[i];
        int stat_index = 0;
        TRY(write_vint64(socket, group-previous_group));
        previous_group = group;
        uint32_t prefetch_group;
        int64_t* prefetch_start;
        int prefetch = i+PREFETCH_DISTANCE < term_group_count;
        if (prefetch) {
            prefetch_group = groups[i+PREFETCH_DISTANCE];
            prefetch_start = group_stats+prefetch_group*stats_size;
        }

        for (; stat_index <= num_stats-8; stat_index += 8) {
            int64_t stat;
            stat = group_stats[group*stats_size+stat_order[stat_index]+0];
            TRY(write_svint64(socket, stat));
            stat = group_stats[group*stats_size+stat_order[stat_index]+1];
            TRY(write_svint64(socket, stat));
            stat = group_stats[group*stats_size+stat_order[stat_index]+2];
            TRY(write_svint64(socket, stat));
            stat = group_stats[group*stats_size+stat_order[stat_index]+3];
            TRY(write_svint64(socket, stat));
            stat = group_stats[group*stats_size+stat_order[stat_index]+4];
            TRY(write_svint64(socket, stat));
            stat = group_stats[group*stats_size+stat_order[stat_index]+5];
            TRY(write_svint64(socket, stat));
            stat = group_stats[group*stats_size+stat_order[stat_index]+6];
            TRY(write_svint64(socket, stat));
            stat = group_stats[group*stats_size+stat_order[stat_index]+7];
            TRY(write_svint64(socket, stat));

            if (prefetch) {
                _mm_prefetch(prefetch_start+stat_index, _MM_HINT_T0);
            }
        }
        if (stat_index < num_stats) {
            int64_t* prefetch_address = prefetch_start+stat_index;
            do {
                int64_t stat = group_stats[group*stats_size+stat_index];
                TRY(write_svint64(socket, stat));
                stat_index++;
            } while (stat_index < num_stats);
            _mm_prefetch(prefetch_address, _MM_HINT_T0);
        }
    }
    TRY(write_byte(socket, 0));
    return 0;
}

static size_t prefix_len(struct string_term_s* term, struct string_term_s* previous_term) {
    size_t max = MAX(term->string_term_len, previous_term->string_term_len);
    for (size_t i = 0; i < max; i++) {
        if (term->string_term[i] != previous_term->string_term[i]) return i;
    }
    return max;
}

int write_term_group_stats(struct session_desc* session, struct tgs_desc* tgs, uint32_t* groups, size_t term_group_count) {
    if (tgs->term_type) {
        if (tgs->previous_term->int_term == -1 && tgs->term->int_term == -1) {
            TRY(write_byte(tgs->socket, 0x80));
            TRY(write_byte(tgs->socket, 0));
        } else {
            TRY(write_vint64(tgs->socket, tgs->term->int_term - tgs->previous_term->int_term));
        }
    } else {
        struct string_term_s* term = &tgs->term->string_term;
        struct string_term_s* previous_term = &tgs->previous_term->string_term;
        size_t p_len = prefix_len(term, previous_term);
        TRY(write_vint64(tgs->socket, term->string_term_len - p_len + 1));
        TRY(write_vint64(tgs->socket, term->string_term_len - p_len));
        TRY(write_bytes(tgs->socket, (uint8_t*)(term->string_term + p_len), term->string_term_len - p_len));
    }
    int64_t term_doc_freq = 0;
    for (int i = 0; i < tgs->n_slices; i++) {
        term_doc_freq += tgs->slices[i].n_docs_in_slice;
    }
    TRY(write_svint64(tgs->socket, term_doc_freq));
    int num_stats = session->num_stats;
    size_t stats_size = num_stats <= 2 ? 2 : (num_stats+3)/4*4;
    TRY(write_group_stats(tgs->socket, groups, term_group_count, (int64_t*)tgs->group_stats, num_stats, stats_size, session->stat_order));
    return 0;
}
