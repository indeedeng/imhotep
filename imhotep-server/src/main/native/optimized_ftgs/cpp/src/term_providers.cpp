#include "term_providers.hpp"

namespace imhotep {

    template <typename term_t>
    TermProviders<term_t>::TermProviders(const std::vector<Shard*>&      shards,
                                         const std::vector<std::string>& field_names,
                                         const std::string&              split_dir,
                                         size_t                          num_splits,
                                         ExecutorService&                executor) {

        for (std::vector<std::string>::const_iterator it(field_names.begin());
             it != field_names.end(); ++it) {
            const std::string& field(*it);
            std::vector<term_source_t> sources(term_sources(shards, field));
            this->emplace_back(std::make_pair(field,
                                              TermProvider<term_t>(sources, field,
                                                                   split_dir, num_splits,
                                                                   executor)));
        }
    }

    template
    TermProviders<IntTerm>::TermProviders(const std::vector<Shard*>&      shards,
                                          const std::vector<std::string>& field_names,
                                          const std::string&              split_dir,
                                          size_t                          num_splits,
                                          ExecutorService&                executor);

    template
    TermProviders<StringTerm>::TermProviders(const std::vector<Shard*>&      shards,
                                             const std::vector<std::string>& field_names,
                                             const std::string&              split_dir,
                                             size_t                          num_splits,
                                             ExecutorService&                executor);

} // namespace imhotep
