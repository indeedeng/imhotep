/*
 * entry_point.cpp
 *
 *  Created on: May 26, 2015
 *      Author: darren
 */
#include "entry_point.hpp"

#include <array>
#include <string>
#include <tuple>
#include "jiterator.hpp"
#include "chained_iterator.hpp"
#include "ftgs_runner.hpp"
#include "interleaved_jiterator.hpp"
extern "C" {
    #include "local_session.h"
    #include "imhotep_native.h"
}

namespace imhotep {

    int run(FTGSRunner& runner,
            int nGroups,
            int nMetrics,
            bool only_binary_metrics,
            packed_table_t *sample_table,
            int *socket_fds,
            ExecutorService executorService)
    {
        using int_transform = wrapping_jIterator<TermDescIterator<IntTerm>, op_desc, op_desc>;
        using string_transform = wrapping_jIterator<TermDescIterator<StringTerm>, op_desc, op_desc>;
        using chained_iter_t = ChainedIterator<int_transform, string_transform>;

        size_t num_workers = runner.getNumWorkers();
        for (size_t i = 0; i < num_workers; i ++) {
            // create the worker
            struct worker_desc worker;
            struct session_desc session;
            InterleavedJIterator<chained_iter_t> iter;
            std::tie(worker, session, iter) =
                    runner.build_worker(i, nGroups, nMetrics, only_binary_metrics, sample_table, socket_fds);


            // kick off the worker
            executorService.enqueue(
                    [=](InterleavedJIterator<chained_iter_t> iterator) -> int
            {
                struct worker_desc my_worker = worker;
                struct session_desc my_session = session;
                op_desc op;
                int err;

                //TODO: fix split index

                while (iterator.hasNext()) {
                    int term_type;
                    iterator.next(op);
                    switch (op._operation) {
                        case op_desc::FIELD_START_OPERATION:
                            term_type = op._termDesc.is_int_field()
                                                ? TERM_TYPE_INT
                                                : TERM_TYPE_STRING;
                             err = worker_start_field(&my_worker,
                                                      op._fieldName.c_str(),
                                                      (int) op._fieldName.length(),
                                                      term_type,
                                                      op._splitIndex);
                            break;
                        case op_desc::TGS_OPERATION: {
                            term_type = op._termDesc.is_int_field()
                                                ? TERM_TYPE_INT
                                                : TERM_TYPE_STRING;
                            const char *str_term = op._termDesc.is_int_field()
                                                ? NULL
                                                : op._termDesc.string_term().c_str();
                            int term_len  = op._termDesc.is_int_field()
                                                ? 0
                                                : op._termDesc.string_term().length();
                            err = run_tgs_pass(&my_worker,
                                                   &my_session,
                                                   term_type,
                                                   op._termDesc.int_term(),
                                                   str_term,
                                                   term_len,
                                                   op._termDesc.docid_addresses(),
                                                   op._termDesc.doc_freqs(),
                                                   op._termDesc.count(),
                                                   op._splitIndex);
                            break;
                        }
                        case op_desc::FIELD_END_OPERATION:
                            err = worker_end_field(&my_worker, op._splitIndex);
                            break;
                        case op_desc::NO_MORE_FIELDS_OPERATION:
                            err = worker_end_stream(&my_worker, op._splitIndex);
                            break;
                        default:
                            break;
                    }
                }

                return 0;
            }, iter);
        }

        // wait for workers to finish
        executorService.await_completion();

        return 0;
    }

}

