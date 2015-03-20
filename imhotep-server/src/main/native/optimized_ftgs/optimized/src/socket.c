#include <stdlib.h>
#include <string.h>
#include "imhotep_native.h"

#define DEFUALT_BUFFER_SIZE                         4096


void stream_init(struct ftgs_outstream *stream, uint32_t fd)
{
    stream->socket.socket_fd = fd;
    stream->socket.buffer = calloc(DEFUALT_BUFFER_SIZE, sizeof(uint8_t));
    stream->socket.buffer_len = DEFUALT_BUFFER_SIZE;
    stream->socket.buffer_ptr = 0;
    stream->socket.err = NULL;
    stream->term_type = TERM_TYPE_INT;
    stream->prev_term.int_term = 0;
}

void stream_destroy(struct ftgs_outstream *stream)
{
    stream->socket.socket_fd = -1;
    free(stream->socket.buffer);
    stream->socket.buffer_len = 0;
    stream->socket.buffer_ptr = 0;
    free(stream->socket.err);
    if (stream->prev_term.string_term.term != NULL) {
    	free(stream->prev_term.string_term.term);
    }
}

void socket_capture_error(struct buffered_socket *socket, int code)
{
    if (!socket->err) calloc(1, sizeof(struct runtime_err));
    socket->err->code = code;
    strerror_r(code, socket->err->str, SIZE_OF_ERRSTR);
}


union term_union *term_create(uint8_t term_type,
                              int int_term,
                              char *string_term,
                              int string_term_len)
{
    union term_union *term;

    term = calloc(1, sizeof(union term_union));
    switch(term_type) {
    case TERM_TYPE_STRING:
        term->string_term.term = calloc(string_term_len, sizeof(char));
        term->string_term.len = string_term_len;
        memcpy(term->string_term.term, string_term, string_term_len);
        break;
    case TERM_TYPE_INT:
        term->int_term = int_term;
        break;
    }
    return term;
}

void term_destroy(uint8_t term_type, union term_union *term)
{
    switch(term_type) {
    case TERM_TYPE_STRING:
        free(term->string_term.term);
        free(term);
        break;
    case TERM_TYPE_INT:
        free(term);
        break;
    }
}
