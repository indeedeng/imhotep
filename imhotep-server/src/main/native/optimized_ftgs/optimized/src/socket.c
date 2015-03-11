#include <stdlib.h>
#include <string.h>
#include "imhotep_native.h"

#define DEFUALT_BUFFER_SIZE                         4096


void socket_init(struct buffered_socket *socket, uint32_t fd)
{
    socket->socket_fd = fd;
    socket->buffer = calloc(DEFUALT_BUFFER_SIZE, sizeof(uint8_t));
    socket->buffer_len = DEFUALT_BUFFER_SIZE;
    socket->buffer_ptr = 0;
    socket->err = 0;
}

void socket_destroy(struct buffered_socket *socket)
{
    socket->socket_fd = -1;
    free(socket->buffer);
    socket->buffer_len = 0;
    socket->buffer_ptr = 0;
    free(socket->err);
}

void socket_capture_error(struct buffered_socket *socket, int code)
{
    if (!socket->err) calloc(1, sizeof(struct runtime_err));
    socket->err->code = code;
    strerror_r(code, socket->err->str, SIZE_OF_ERRSTR);
}
