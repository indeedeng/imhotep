#ifndef FTGS_RUNNER_HPP
#define FTGS_RUNNER_HPP

#include <algorithm>
#include <iterator>
#include <utility>
#include <vector>

#include "executor_service.hpp"
#include "shard.hpp"
#include "term_providers.hpp"

namespace imhotep {

    class FTGSRunner {
    public:
        FTGSRunner(const std::vector<Shard*>&      shards,
                   const std::vector<std::string>& int_fieldnames,
                   const std::vector<std::string>& string_fieldnames,
                   const std::string&              split_dir,
                   size_t                          num_splits,
                   size_t                          num_workers,
                   ExecutorService&                executor);

        FTGSRunner(const FTGSRunner& rhs) = delete;

        void run(int                     num_groups,
                 int                     num_metrics,
                 bool                    only_binary_metrics,
                 Shard::packed_table_ptr sample_table,
                 const std::vector<int>& socket_fds);

    private:
        const std::vector<Shard*>       _shards;
        const TermProviders<IntTerm>    _int_term_providers;
        const TermProviders<StringTerm> _string_term_providers;
        const size_t                    _num_splits;
        const size_t                    _num_workers;

        ExecutorService& _executor;
    };

} // namespace imhotep

#endif
