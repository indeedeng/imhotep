#include <stdlib.h>
#include "imhotep_native.h"

#define DEFUALT_BUFFER_SIZE                         4096


void socket_init(struct buffered_socket *socket, uint32_t fd)
{
    socket->socket_fd = fd;
    socket->buffer = calloc(DEFUALT_BUFFER_SIZE, sizeof(uint8_t));
    socket->buffer_len = DEFUALT_BUFFER_SIZE;
    socket->buffer_ptr = 0;
}

void socket_destroy(struct buffered_socket *socket)
{
    socket->socket_fd = -1;
    free(socket->buffer);
    socket->buffer_len = 0;
    socket->buffer_ptr = 0;
}

