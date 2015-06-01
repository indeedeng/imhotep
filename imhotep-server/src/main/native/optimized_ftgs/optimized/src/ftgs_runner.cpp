#define BOOST_RESULT_OF_USE_DECLTYPE    1

#include <tuple>
#include "ftgs_runner.hpp"

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


    FTGSRunner::FTGSRunner(const std::vector<Shard>&       shards,
                           const std::vector<std::string>& int_fieldnames,
                           const std::vector<std::string>& string_fieldnames,
                           const std::string&              split_dir,
                           size_t                          num_splits,
                           size_t                          num_workers,
                           ExecutorService&                executor)
        : _shards(shards)
        , _int_fieldnames(int_fieldnames)
        , _string_fieldnames(string_fieldnames)
        , _int_term_providers(shards, int_fieldnames, split_dir, num_splits, executor)
        , _string_term_providers(shards, string_fieldnames, split_dir, num_splits, executor)
        , _num_splits(num_splits)
        , _num_workers(num_workers)
    { }

    template<typename term_t>
    auto FTGSRunner::forSplit_getFieldIters(const TermProviders<term_t>& providers,
                                            int split_num)
        -> std::vector<wrapping_jIterator<TermDescIterator<term_t>, op_desc, op_desc>>
    {
        using transform_t = wrapping_jIterator<TermDescIterator<term_t>, op_desc, op_desc>;
        std::vector<transform_t> term_desc_iters;

        for (size_t i = 0; i < providers.size(); i++) {
            auto provider = providers[i].second;

            op_desc func(i, op_desc::TGS_OPERATION);
            const auto wrapped_provider = transform_t(provider.merge(split_num), func);
            term_desc_iters.push_back(wrapped_provider);
        }

        return term_desc_iters;
    }

    template <typename iter1_t, typename iter2_t>
    auto FTGSRunner::forSplit_chainFieldIters(std::vector<iter1_t>& term_iter1,
                                              std::vector<iter2_t>& term_iter2,
                                              const int split_num)
            -> ChainedIterator<iter1_t, iter2_t>
    {
        // create chained iterator for all fields
        auto int_fields = getIntFieldnames();
        auto string_fields = getStringFieldnames();

        return
            ChainedIterator<iter1_t, iter2_t>
                (term_iter1,
                 term_iter2,
                [=](int32_t field_num) -> op_desc
                     {
                         const std::string& field = ((size_t)field_num < int_fields.size())
                         ? int_fields[field_num]
                         : string_fields[field_num - int_fields.size()];
                         return op_desc(split_num, op_desc::FIELD_START_OPERATION, field);
                     },
                [=](int32_t field_num) -> op_desc
                     {
                         return op_desc(split_num, op_desc::FIELD_END_OPERATION);
                     },
                [=]() -> op_desc
                    {
                         return op_desc(split_num, op_desc::NO_MORE_FIELDS_OPERATION);
                    }
                );
    }

} // namespace imhotep
