#include "term_providers.hpp"

namespace imhotep {

    template <typename term_t>
    TermProviders<term_t>::TermProviders(const std::vector<Shard>&       shards,
                                         const std::vector<std::string>& field_names,
                                         const std::string&              split_dir,
                                         size_t                          num_splits,
                                         ExecutorService&                executor) {
        std::transform(field_names.begin(), field_names.end(),
                       std::back_inserter<TermProviders>(*this),
                       [this, &shards, &split_dir, num_splits, &executor](const std::string& field) {
                           std::vector<term_source_t> sources(term_sources(shards, field));
                           return std::make_pair(field, TermProvider<term_t>(sources, field,
                                                                             split_dir, num_splits,
                                                                             executor));
                       });
    }

    template
    TermProviders<IntTerm>::TermProviders(const std::vector<Shard>&       shards,
                                          const std::vector<std::string>& field_names,
                                          const std::string&              split_dir,
                                          size_t                          num_splits,
                                          ExecutorService&                executor);

    template
    TermProviders<StringTerm>::TermProviders(const std::vector<Shard>&       shards,
                                             const std::vector<std::string>& field_names,
                                             const std::string&              split_dir,
                                             size_t                          num_splits,
                                             ExecutorService&                executor);

} // namespace imhotep
