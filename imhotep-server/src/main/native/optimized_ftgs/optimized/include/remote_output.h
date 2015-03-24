#pragma once

#include <stdint.h>
#include "imhotep_native.h"

int write_field_start(struct ftgs_outstream* stream,
                char *field_name,
                int len,
                int term_type);
int write_field_end(struct ftgs_outstream* stream);

int write_term_group_stats(const struct session_desc* session,
                           const struct tgs_desc* tgs,
                           const uint32_t* restrict groups,
                           const size_t term_group_count);
int flush_buffer(struct buffered_socket* socket);
