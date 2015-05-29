/*
 * entry_point.cpp
 *
 *  Created on: May 26, 2015
 *      Author: darren
 */
#include "entry_point.hpp"

#include <array>
#include <string>
#include "jiterator.hpp"
#include "chained_iterator.hpp"
#include "ftgs_runner.hpp"
#include "interleaved_jiterator.hpp"
extern "C" {
    #include "local_session.h"
    #include "imhotep_native.h"
}

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

        op_desc(int32_t splitIndex, int8_t operation, std::string field) :
                _splitIndex(splitIndex),
                _operation(operation),
                _fieldName(field)
        { }

        op_desc& operator()(TermDesc& desc) {
            _termDesc = desc;
            return *this;
        }

    };


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

    template<typename iterator_t>
    int run_worker(struct worker_desc *worker, iterator_t iterator) {
        op_desc op;

        while (iterator.hasNext()) {
            iterator.next(op);
            switch (op._operation) {
                case op_desc::FIELD_START_OPERATION:
                    break;
                case op_desc::TGS_OPERATION:
                    break;
                case op_desc::FIELD_END_OPERATION:
                    break;
                case op_desc::NO_MORE_FIELDS_OPERATION:
                    break;
                default:
                    break;
            }
        }

        return 0;
    }

    int run(FTGSRunner& runner, int nGroups, int nMetrics, int *socket_fds) {
        using int_transform = transform_jIterator<TermDescIterator<IntTerm>, op_desc>;
        using string_transform = transform_jIterator<TermDescIterator<StringTerm>, op_desc>;
        using chained_iter_t = ChainedIterator<int_transform, string_transform>;

        std::vector<chained_iter_t> split_iters(runner.getNumSplits());

        for (size_t i = 0; i < runner.getNumSplits(); i++) {

            // create chained iterator for the field
            auto int_fields = runner.getIntFieldnames();
            auto string_fields = runner.getStringFieldnames();
            std::function<op_desc (int32_t)> f1 = [=](int32_t field_num) -> op_desc
                    {
                        const std::string& field = ((size_t)field_num < int_fields.size())
                                ? int_fields[field_num]
                                : string_fields[field_num - int_fields.size()];
                        return op_desc(i, op_desc::FIELD_START_OPERATION, field);
                    };
            std::function<op_desc (int32_t)> f2 = [=](int32_t field_num) -> op_desc
                    {
                        return op_desc(i, op_desc::FIELD_END_OPERATION);
                    };

            // get split iterators for every field
            auto chained_splits = ChainedIterator<int_transform, string_transform>(
                    build_term_iters(runner.getIntTermProviders(), i),
                    build_term_iters(runner.getStringTermProviders(), i),
                    f1,
                    f2);

            // save the iterator
            split_iters.push_back(chained_splits);
        }

        // create the workers
        size_t num_workers = runner.getNumWorkers();
        size_t num_streams_per_worker = runner.getNumSplits() / num_workers;
        size_t num_plus_one_workers = runner.getNumSplits() % num_workers;

        std::vector<struct worker_desc> workers(num_workers);
        std::vector<InterleavedJIterator<chained_iter_t>> final_iters(num_workers);
        int socket_offset = 0;
        for (size_t i = 0; i < num_plus_one_workers; i++) {
            worker_init(&workers[i],
                        socket_offset,
                        nGroups,
                        nMetrics,
                        &socket_fds[socket_offset],
                        num_streams_per_worker + 1);

            //TODO: create session

            for (size_t j = 0; j < num_streams_per_worker + 1; j++) {
                auto iter = InterleavedJIterator<chained_iter_t>(split_iters.begin()
                                                                     + socket_offset,
                                                                 split_iters.begin()
                                                                     + socket_offset
                                                                     + num_streams_per_worker + 1);
                final_iters.push_back(iter);
            }

            socket_offset += num_streams_per_worker + 1;
        }
        for (size_t i = num_plus_one_workers; i < num_workers; i++) {
            worker_init(&workers[i],
                        socket_offset,
                        nGroups,
                        nMetrics,
                        &socket_fds[socket_offset],
                        num_streams_per_worker);

            //TODO: create session

            for (size_t j = 0; j < num_streams_per_worker; j++) {
                auto iter = InterleavedJIterator<chained_iter_t>(split_iters.begin()
                                                                     + socket_offset,
                                                                 split_iters.begin()
                                                                     + socket_offset
                                                                     + num_streams_per_worker);
                final_iters.push_back(iter);
            }

            socket_offset += num_streams_per_worker;
        }


        // kick off the workers

        return 0;
    }

}

