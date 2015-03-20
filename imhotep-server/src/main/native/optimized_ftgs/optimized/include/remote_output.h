#pragma once

#include <stdint.h>
#include "imhotep_native.h"

int write_field(struct ftgs_outstream* stream,
                unpacked_table_t *table,
                char *field_name,
                int len,
                int term_type);
int write_term_group_stats(struct session_desc* session, struct tgs_desc* tgs, uint32_t* groups, size_t term_group_count);
