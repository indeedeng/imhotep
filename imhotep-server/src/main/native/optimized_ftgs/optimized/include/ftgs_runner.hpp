#ifndef FTGS_RUNNER_HPP
#define FTGS_RUNNER_HPP

#include <algorithm>
#include <iterator>
#include <utility>
#include <vector>

#include "executor_service.hpp"
#include "shard.hpp"
#include "term_provider.hpp"

namespace imhotep {

    template <typename term_t>
    class TermProviders : public std::vector<std::pair<std::string, TermProvider<term_t>>> {
    public:
        TermProviders(const std::vector<Shard>&       shards,
                      const std::vector<std::string>& field_names,
                      const std::string&              split_dir,
                      size_t                          num_splits,
                      ExecutorService&                executor);

    private:
        typedef TermIterator<term_t>      term_it;
        typedef std::pair<Shard, term_it> term_source_t;

        std::vector<term_source_t> term_sources(const std::vector<Shard>& shards,
                                                const std::string&        field) const {
            std::vector<term_source_t> result;
            std::transform(shards.begin(), shards.end(),
                           std::back_inserter(result),
                           [&field](const Shard& shard) {
                               return std::make_pair(shard, term_it(shard, field));
                           });
            return result;
        }
    };

    class FTGSRunner {
    public:
        /** todo(johnf): incorporate ExecutorService
            todo(johnf): plumb docid base addresses through (or wire that up somehow)
         */
        FTGSRunner(const std::vector<Shard>&       shards,
                   const std::vector<std::string>& int_fieldnames,
                   const std::vector<std::string>& string_fieldnames,
                   const std::string&              split_dir,
                   size_t                          num_splits,
                   size_t                          num_workers,
                   ExecutorService&                executor);

        FTGSRunner(const FTGSRunner& rhs) = delete;

        const std::vector<std::string>& getIntFieldnames() const
        {
            return _int_fieldnames;
        }

        const TermProviders<IntTerm>& getIntTermProviders() const
        {
            return _int_term_providers;
        }

        const size_t getNumSplits() const
        {
            return _num_splits;
        }

        const size_t getNumWorkers() const
        {
            return _num_workers;
        }

        const std::vector<std::string>& getStringFieldnames() const
        {
            return _string_fieldnames;
        }

        const TermProviders<StringTerm>& getStringTermProviders() const
        {
            return _string_term_providers;
        }

    private:
        template<typename term_t>
        auto build_term_iters(TermProviders<term_t> providers);

        const std::vector<std::string>& _int_fieldnames;
        const std::vector<std::string>& _string_fieldnames;
        const TermProviders<IntTerm>    _int_term_providers;
        const TermProviders<StringTerm> _string_term_providers;
        const size_t _num_splits;
        const size_t _num_workers;
    };

} // namespace imhotep

#endif
