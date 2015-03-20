#pragma once

#include <stdint.h>
#include "imhotep_native.h"

int write_field_start(struct ftgs_outstream* stream,
                char *field_name,
                int len,
                int term_type);
int write_field_end(struct ftgs_outstream* stream);

int write_term_group_stats(struct session_desc* session, struct tgs_desc* tgs, uint32_t* groups, size_t term_group_count);
int flush_buffer(struct buffered_socket* socket);
