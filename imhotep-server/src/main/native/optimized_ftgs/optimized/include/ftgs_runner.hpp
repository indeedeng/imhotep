#ifndef FTGS_RUNNER_HPP
#define FTGS_RUNNER_HPP

#include <algorithm>
#include <iterator>
#include <utility>
#include <vector>

#include "term_provider.hpp"

namespace imhotep {

    template <typename term_t>
    class TermProviders : public std::vector<std::pair<std::string, TermProvider<term_t>>> {
    public:
        TermProviders(const std::vector<std::string>& shard_dirs,
                      const std::vector<std::string>& field_names,
                      const std::string&              split_dir,
                      size_t                          num_splits);

    private:
        typedef TermIterator<term_t>            term_it;
        typedef std::pair<std::string, term_it> term_source_t;

        std::vector<term_source_t> term_sources(const std::vector<std::string>& shard_dirs,
                                                const std::string&              field) const {
            std::vector<term_source_t> result;
            std::transform(shard_dirs.begin(), shard_dirs.end(),
                           std::back_inserter(result),
                           [&field](const std::string& shard_dir) {
                               term_it it(Shard::term_filename<term_t>(shard_dir, field));
                               return std::make_pair(Shard::name_of(shard_dir), it);
                           });
            return result;
        }
    };

    class FTGSRunner {
    public:
        /** todo(johnf): incorporate ExecutorService
            todo(johnf): add packed_table references (pointers)
            todo(johnf): plumb docid base addresses through (or wire that up somehow)
         */
        FTGSRunner(const std::vector<std::string>& shard_dirs,
                   const std::vector<std::string>& int_fieldnames,
                   const std::vector<std::string>& string_fieldnames,
                   const std::string&              split_dir,
                   size_t                          num_splits);

        FTGSRunner(const FTGSRunner& rhs) = delete;

        void operator()();

    private:

        TermProviders<IntTerm>    _int_term_providers;
        TermProviders<StringTerm> _string_term_providers;
    };

} // namespace imhotep

#endif
