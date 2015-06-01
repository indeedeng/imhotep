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
            FTGSRunner::split_handle_t handle;
            std::tie(worker, session, iter, handle) = runner.build_worker(i,
                                                                          nGroups,
                                                                          nMetrics,
                                                                          only_binary_metrics,
                                                                          sample_table,
                                                                          socket_fds);


            // kick off the worker
            executorService.enqueue(
                    [=](InterleavedJIterator<chained_iter_t> iterator) -> int
            {
                struct worker_desc my_worker = worker;
                struct session_desc my_session = session;
                op_desc op;
                int err __attribute__((unused));

                while (iterator.hasNext()) {
                    iterator.next(op);
                    int socket_num = FTGSRunner::forWorker_getSplitOrdinal(handle, op._splitIndex);
                    switch (op.operation()) {
                        case op_desc::FIELD_START_OPERATION:
                            err = worker_start_field(&my_worker,
                                                     op.field_name().c_str(),
                                                     (int) op.field_name().length(),
                                                     op.term_type(),
                                                      socket_num);
                            break;
                        case op_desc::TGS_OPERATION: {
                            err = run_tgs_pass(&my_worker,
                                               &my_session,
                                               op.term_type(),
                                               op.int_term(),
                                               op.str_term(),
                                               op.str_term_length(),
                                               op.term_desc().docid_addresses(),
                                               op.term_desc().doc_freqs(),
                                               const_cast<const packed_table_t **>(op.term_desc().tables()),
                                               op.term_desc().count(),
                                               socket_num);
                            break;
                        }
                        case op_desc::FIELD_END_OPERATION:
                            err = worker_end_field(&my_worker, socket_num);
                            break;
                        case op_desc::NO_MORE_FIELDS_OPERATION:
                            err = worker_end_stream(&my_worker, socket_num);
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

