#ifndef ENTRY_POINT_HPP
#define ENTRY_POINT_HPP

#include "ftgs_runner.hpp"

namespace imhotep {

    int run(FTGSRunner& runner, int nGroups, int nMetrics, int *socket_fds);

} // namespace imhotep

#endif
