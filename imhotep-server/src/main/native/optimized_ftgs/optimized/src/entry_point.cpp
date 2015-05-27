/*
 * entry_point.cpp
 *
 *  Created on: May 26, 2015
 *      Author: darren
 */
#include <tuple>
#include "chained_iterator.hpp"
#include "ftgs_runner.hpp"
#include "jiterator.hpp"

namespace imhotep {

    class op_desc {
    public:
        static const int8_t TGS_OPERATION = 1;
        static const int8_t FIELD_START_OPERATION = 2;
        static const int8_t FIELD_END_OPERATION = 3;
        static const int8_t NO_MORE_FIELDS_OPERATION = 4;

        TermDesc     _termDesc;
        int32_t      _splitIndex;
        int8_t       _operation;
        std::string  _fieldName;

        op_desc() :
            _splitIndex(-1),
            _operation(0)
        { }

        op_desc(int32_t splitIndex, int8_t operation) :
                _splitIndex(splitIndex),
                _operation(operation)
        { }

        op_desc& operator()(TermDesc& desc) {
            this->_termDesc = desc;
            return *this;
        }

    };

//    class op_func {
//    public:
//        op_func() { }
//
//        op_func(int32_t splitIndex, int8_t operation) : _op_desc(splitIndex, operation) { }
//
//        op_desc& operator()(const TermDesc& desc) const {
//            _op_desc._termDesc = desc;
//            return _op_desc;
//        }
//
//    private:
//        op_desc _op_desc;
//    };


    template<typename term_t>
    auto build_term_iters(const TermProviders<term_t>& providers, int split_num) ->
        std::vector<transform_jIterator<TermDescIterator<term_t>, op_desc>>
    {
        using transform_t = transform_jIterator<TermDescIterator<term_t>, op_desc>;

        std::vector<transform_t> term_desc_iters;

        for (size_t i = 0; i < providers.size(); i++) {
            auto provider = providers[i].second;

            op_desc func(i, op_desc::TGS_OPERATION);
            const auto wrapped_provider = transform_t(provider.merge(split_num), func);
            term_desc_iters.push_back(wrapped_provider);
        }

        return term_desc_iters;
    }

    int run(FTGSRunner& runner) {
        using int_transform = transform_jIterator<TermDescIterator<IntTerm>, op_desc>;
        using string_transform = transform_jIterator<TermDescIterator<StringTerm>, op_desc>;

        std::vector<ChainedIterator<int_transform, string_transform>> split_iters;

        for (size_t i = 0; i < runner.getNumSplits(); i++) {

            auto int_term_desc_iters = build_term_iters(runner.getIntTermProviders(), i);
            auto string_term_desc_iters =
                    build_term_iters<StringTerm>(runner.getStringTermProviders(), i);

            // get split iterators for every field
//            std::vector<int_transform> int_term_desc_iters;
//            std::vector<string_transform> string_term_desc_iters;
//
//            for (int j = 0; j < runner._int_term_providers.size(); j++) {
//                op_desc func(i, op_desc::TGS_OPERATION);
//                auto provider = _int_term_providers[j].second;
//                const auto j_provider = iterator_2_jIterator(provider.merge(i));
//                const auto wrapped_provider = int_transform(j_provider, func);
//                int_term_desc_iters.push_back(wrapped_provider);
//            }
//            for (int j = 0; j < _string_term_providers.size(); j++) {
//                op_desc func(i, op_desc::TGS_OPERATION);
//                auto provider = _string_term_providers[j].second;
//                const auto j_provider = iterator_2_jIterator(provider.merge(i));
//                const auto wrapped_provider = string_transform(j_provider, func);
//                string_term_desc_iters.push_back(wrapped_provider);
//            }

            // create chained iterator for the field
            std::function<op_desc (int32_t num)> f1 = [=](int32_t num) -> op_desc
                    {
                        return op_desc(i, op_desc::FIELD_START_OPERATION);
                    };
            std::function<op_desc (int32_t num)> f2 = [=](int32_t num) -> op_desc
                    {
                        return op_desc(i, op_desc::FIELD_END_OPERATION);
                    };

            auto chained_splits = ChainedIterator<int_transform, string_transform>(
                    int_term_desc_iters,
                    string_term_desc_iters,
                    f1,
                    f2);

            // save the iterator
            split_iters.push_back(chained_splits);
        }

        return 0;
    }

}

