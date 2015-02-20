#include <mmintrin.h>
#include <unistd.h>
#include "remote_output.h"

#define PREFETCH_DISTANCE 16

// TODO: throw an exception with JNI

#define TRY(a) { \
    int _err = (a); \
    if (_err != 0) return _err; \
}

static int write_byte(struct socket_stuff* socket, uint8_t value) {
    socket->buffer[socket->buffer_ptr] = value;
    socket->buffer_ptr++;
    if (socket->buffer_ptr == socket->buffer_len) {
        size_t write_ptr = 0;
        while (write_ptr < socket->buffer_ptr) {
            ssize_t written = write(socket->socket_fd, socket->buffer, socket->buffer_len);
            if (written == -1) return -1;
            write_ptr += written;
        }
    }
    return 0;
}

static int write_vint64(struct socket_stuff* socket, uint64_t i) {
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

static int write_svint64(struct socket_stuff* socket, int64_t i) {
    return write_vint64(socket, (i << 1) ^ (i >> 63));
}

int write_group_stats(struct socket_stuff* socket, uint32_t* groups, size_t groups_present,
             int64_t* group_stats, int num_stats, size_t stats_size) {
    int32_t previous_group = -1;
    for (size_t i = 0; i < groups_present; i++) {
        uint32_t group = groups[i];
        int stat_index = 0;
        TRY(write_vint64(socket, group-previous_group));
        previous_group = group;
        uint32_t prefetch_group;
        int64_t* prefetch_start;
        int prefetch = i+PREFETCH_DISTANCE < groups_present;
        if (prefetch) {
            prefetch_group = groups[i+PREFETCH_DISTANCE];
            prefetch_start = group_stats+prefetch_group*stats_size;
        }
        
        for (; stat_index <= num_stats-8; stat_index += 8) {
            int64_t stat = group_stats[group*stats_size+stat_index];
            TRY(write_svint64(socket, stat));
            stat = group_stats[group*stats_size+stat_index+1];
            TRY(write_svint64(socket, stat));
            stat = group_stats[group*stats_size+stat_index+2];
            TRY(write_svint64(socket, stat));
            stat = group_stats[group*stats_size+stat_index+3];
            TRY(write_svint64(socket, stat));
            stat = group_stats[group*stats_size+stat_index+4];
            TRY(write_svint64(socket, stat));
            stat = group_stats[group*stats_size+stat_index+5];
            TRY(write_svint64(socket, stat));
            stat = group_stats[group*stats_size+stat_index+6];
            TRY(write_svint64(socket, stat));
            stat = group_stats[group*stats_size+stat_index+7];
            TRY(write_svint64(socket, stat));
            
            if (prefetch) {
                _mm_prefetch(prefetch_start+stat_index, _MM_HINT_T0);
            }
        }
        if (stat_index < num_stats) {
            do {
                int64_t stat = group_stats[group*stats_size+stat_index];
                TRY(write_svint64(socket, stat));
                stat_index++;
            } while (stat_index < num_stats);
            _mm_prefetch(prefetch_start+stat_index, _MM_HINT_T0);
        }
    }
    TRY(write_byte(socket, 0));
    return 0;
}
