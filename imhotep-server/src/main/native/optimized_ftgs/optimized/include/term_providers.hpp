#ifndef TERM_PROVIDERS_HPP
#define TERM_PROVIDERS_HPP

#include <string>
#include <vector>

#include "shard.hpp"
#include "term_iterator.hpp"
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
            for (std::vector<Shard>::const_iterator it(shards.begin());
                 it != shards.end(); ++it) {
                const Shard& shard(*it);
                result.emplace_back(std::make_pair(shard, term_it(shard, field)));
            }
            return result;
        }
    };

} // namespace imhotep

#endif
