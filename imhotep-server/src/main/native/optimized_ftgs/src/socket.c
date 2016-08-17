#include <stdlib.h>
#include <string.h>
#include "imhotep_native.h"

#define DEFUALT_BUFFER_SIZE 4096


void stream_init(struct ftgs_outstream *stream, uint32_t fd)
{
    stream->socket.socket_fd = fd;
    stream->socket.buffer = calloc(DEFUALT_BUFFER_SIZE, sizeof(uint8_t));
    stream->socket.buffer_len = DEFUALT_BUFFER_SIZE;
    stream->socket.buffer_ptr = 0;
    stream->socket.err.code = 0;
    stream->term_type = TERM_TYPE_INT;
    stream->prev_term.int_term = -1;
    stream->prev_term.string_term.len = 0;
    stream->prev_term.string_term.term = NULL;
}

void stream_destroy(struct ftgs_outstream *stream)
{
    stream->socket.socket_fd = -1;
    free(stream->socket.buffer);
    stream->socket.buffer_len = 0;
    stream->socket.buffer_ptr = 0;
}

void socket_capture_error(struct buffered_socket *socket, const int code)
{
    socket->err.code = code;
    strerror_r(code, socket->err.str, SIZE_OF_ERRSTR);
}

/* term_s functions. Note that string terms do not make copies of the strings
   handed to them, which are managed externally. (In fact the strings in
   question refer to regions in memory mapped split files.) */

void term_init(struct term_s *term,
               const uint8_t term_type,
               const long int_term,
               const char *string_term,
               const int string_term_len)
{
    switch(term_type) {
    case TERM_TYPE_STRING:
        term->string_term.len  = string_term_len;
        term->string_term.term = (char*) string_term;
        break;
    case TERM_TYPE_INT:
        term->int_term = int_term;
        break;
    }
}

void term_destroy(uint8_t term_type, struct term_s *term)
{
}

void term_update_int(struct term_s *term, struct term_s *new_term)
{
    term->int_term = new_term->int_term;
}

void term_update_string(struct term_s *term, struct term_s *new_term)
{
    term->string_term = new_term->string_term;
}

void term_reset(struct term_s *term)
{
    term->int_term = -1;
    term->string_term.len = 0;
    term->string_term.term = NULL;
}
