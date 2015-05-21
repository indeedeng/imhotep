#include "ftgs_runner.hpp"

namespace imhotep {

    template <typename term_t>
    TermProviders<term_t>::TermProviders(const std::vector<std::string>& shard_dirs,
                                         const std::vector<std::string>& field_names,
                                         const std::string&              split_dir,
                                         size_t                          num_splits) {
        std::transform(field_names.begin(), field_names.end(),
                       std::back_inserter<TermProviders>(*this),
                       [this, &shard_dirs, &split_dir, num_splits](const std::string& field) {
                           std::vector<term_source_t> sources(term_sources(shard_dirs, field));
                           return std::make_pair(field, TermProvider<term_t>(sources, field, split_dir, num_splits));
                       });
    }

    template
    TermProviders<IntTerm>::TermProviders(const std::vector<std::string>& shard_dirs,
                                          const std::vector<std::string>& field_names,
                                          const std::string&              split_dir,
                                          size_t                          num_splits);

    template
    TermProviders<StringTerm>::TermProviders(const std::vector<std::string>& shard_dirs,
                                             const std::vector<std::string>& field_names,
                                             const std::string&              split_dir,
                                             size_t                          num_splits);


    FTGSRunner::FTGSRunner(const std::vector<std::string>& shard_dirs,
                           const std::vector<std::string>& int_fieldnames,
                           const std::vector<std::string>& string_fieldnames,
                           const std::string&              split_dir,
                           size_t                          num_splits)
        : _int_term_providers(shard_dirs, int_fieldnames, split_dir, num_splits)
        , _string_term_providers(shard_dirs, string_fieldnames, split_dir, num_splits)
    { }

    void FTGSRunner::operator()() {
    }

} // namespace imhotep
