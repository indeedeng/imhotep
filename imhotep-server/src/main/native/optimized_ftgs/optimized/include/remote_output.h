#pragma once

#include <stdint.h>
#include <imhotep_native.h>
#include "imhotep_native.h"

int write_term_group_stats(struct session_desc* session, struct tgs_desc* tgs, uint32_t* groups, size_t term_group_count);
