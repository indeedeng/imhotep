#ifndef ENTRY_POINT_HPP
#define ENTRY_POINT_HPP

#include "ftgs_runner.hpp"

namespace imhotep {

    class ImhotepException : public std::runtime_error {
        using std::runtime_error::runtime_error;
    };

    int run(FTGSRunner& runner,
            int nGroups,
            int nMetrics,
            bool only_binary_metrics,
            const packed_table_t *sample_table,
            int *socket_fds,
            ExecutorService& executorService);

} // namespace imhotep

#endif
