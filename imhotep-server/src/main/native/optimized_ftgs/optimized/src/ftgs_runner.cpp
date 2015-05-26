#define BOOST_RESULT_OF_USE_DECLTYPE    1

#include <tuple>
#include <functional>
#include <boost/iterator/transform_iterator.hpp>
#include "ChainedIterator.hpp"
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

    class op_desc {
    public:
        static const int8_t TGS_OPERATION = 1;
        static const int8_t FIELD_START_OPERATION = 2;
        static const int8_t FIELD_END_OPERATION = 3;
        static const int8_t NO_MORE_FIELDS_OPERATION = 4;
        
        TermDesc    _termDesc;
        int32_t      _splitIndex;
        int8_t       _operation;
        std::string _fieldName;

        op_desc() { }
        
        op_desc(int32_t splitIndex, int8_t operation) :
                _splitIndex(splitIndex),
                _operation(operation)
        { }
    };
    
    class op_func {
    public:
        typedef op_desc result_type;
        
        op_func() { }
        
        op_func(int32_t splitIndex, int8_t operation) : _op_desc(splitIndex, operation) { }

        const op_desc& operator()(const TermDesc& desc) const {
            _op_desc._termDesc = desc;
            return _op_desc;
        }
        
    private:
        mutable op_desc _op_desc;
    };


    FTGSRunner::FTGSRunner(const std::vector<Shard>&       shards,
                           const std::vector<std::string>& int_fieldnames,
                           const std::vector<std::string>& string_fieldnames,
                           const std::string&              split_dir,
                           size_t                          num_splits,
                           size_t                          num_workers,
                           ExecutorService&                executor)
        : _int_term_providers(shards, int_fieldnames, split_dir, num_splits, executor)
        , _string_term_providers(shards, string_fieldnames, split_dir, num_splits, executor)
        , _num_splits(num_splits)
        , _num_workers(num_workers)
        , _int_fieldnames(int_fieldnames)
        , _string_fieldnames(string_fieldnames)
    { }

    void FTGSRunner::operator()() {
        using int_transform =
                typename boost::transform_iterator<
                                    op_func,
                                    TermDescIterator<MergeIterator<IntTerm>>
                           >;
        using string_transform =
                typename boost::transform_iterator<
                                    op_func,
                                    TermDescIterator<MergeIterator<StringTerm>>
                           >;

        for (int i = 0; i < _num_splits; i++) {

            // get split iterators for every field
            std::vector<std::pair<int_transform, int_transform>> int_term_desc_iters;
            std::vector<std::pair<string_transform,string_transform>> string_term_desc_iters;

            for (int j = 0; j < _int_term_providers.size(); j++) {
                op_func func(i, op_desc::TGS_OPERATION);
                auto provider = _int_term_providers[j].second;
                TermDescIterator<MergeIterator<IntTerm>> provider_end;
                const auto wrapped_provider =
                        boost::make_transform_iterator(provider.merge(i), func);
                int_term_desc_iters.push_back( {wrapped_provider, wrapped_end} );
            }
            for (int j = 0; j < _string_term_providers.size(); j++) {
                op_func func(i, op_desc::TGS_OPERATION);
                auto provider = _string_term_providers[j].second;
                TermDescIterator<MergeIterator<StringTerm>> provider_end;
                const auto wrapped_provider =
                        boost::make_transform_iterator(provider.merge(i), func);
                string_term_desc_iters.push_back( {wrapped_provider, wrapped_end} );
            }

            auto splits = std::make_pair(int_term_desc_iters, string_term_desc_iters);

            // create chained iterator for the field
            auto chained_splits = ChainedIterator<int_transform, string_transform>(
                    splits,
                    [=](int32_t num) -> op_desc {
                        return op_desc(i, op_desc::FIELD_START_OPERATION);
                    },
                    [=](int32_t num) -> op_desc {
                        return op_desc(i, op_desc::FIELD_END_OPERATION);
                    });

            // save the iterator
        }
    }

} // namespace imhotep
