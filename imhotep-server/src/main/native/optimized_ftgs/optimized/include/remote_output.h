#include <stdint.h>
#include <imhotep_native.h>

int write_tgs(struct socket_stuff* socket, uint32_t* groups, size_t groups_present,
             int64_t** group_stats, size_t num_groups, size_t num_stats);
