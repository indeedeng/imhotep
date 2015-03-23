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


struct term_s *term_create(uint8_t term_type,
                              int int_term,
                              char *string_term,
                              int string_term_len)
{
    struct term_s *term;

    term = calloc(1, sizeof(struct term_s));
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

void term_destroy(uint8_t term_type, struct term_s *term)
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

void term_update_int(struct term_s *term, struct term_s *new_term)
{
    term->int_term = new_term->int_term;
}

void term_update_string(struct term_s *term, struct term_s *new_term)
{
	int new_len = new_term->string_term.len;
	int current_len = term->string_term.len;

	if (new_len > current_len) {
        /* reallocate the string buffer */
        char *new_buf;
        new_buf = malloc(new_len * sizeof(char));
    	memcpy(new_buf, new_term->string_term.term, new_len);
    	free(term->string_term.term);
        term->string_term.term = new_buf;
	}
    term->string_term.len = new_len;
    memcpy(term->string_term.term, new_term->string_term.term, new_len);
}

void term_reset(struct term_s *term)
{
    term->int_term = -1;
    term->string_term.len = 0;
    free(term->string_term.term);
}
