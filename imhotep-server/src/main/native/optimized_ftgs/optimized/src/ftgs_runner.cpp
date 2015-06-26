#include "ftgs_runner.hpp"

#include <iostream>
#include <sstream>
#include <tuple>

#include <gperftools/profiler.h>

#include "imhotep_error.hpp"
#include "log.hpp"
#include "task_iterator.hpp"

extern "C" {
#include "local_session.h"
#include "imhotep_native.h"
}

namespace imhotep {

    class SplitRanges : public std::vector<std::pair<size_t, size_t>> {
    public:
        typedef std::pair<size_t, size_t> Range;

        SplitRanges(size_t num_splits, size_t num_workers) {
            const size_t streams_per_worker(num_splits / num_workers);
            int remainder(num_splits % num_workers);
            std::pair<size_t, size_t> current(std::make_pair(0, streams_per_worker +
                                                             (remainder > 0 ? 1 : 0) - 1));
            size_t index(0);
            while (index < num_workers) {
                push_back(current);
                --remainder;
                current.first = current.second + 1;
                current.second = current.first + streams_per_worker +
                    (remainder > 0 ? 1 : 0) - 1;
                ++index;
            }
        }

        const Range& splits_for(size_t worker_num) const { return at(worker_num); }
    };

    std::ostream& operator<<(std::ostream& os, const SplitRanges& ranges) {
        for (auto range: ranges) {
            os << "(" << range.first << ".." << range.second << ") ";
        }
        return os;
    }

    class Worker {
    public:
        Worker(size_t                           id,
               const SplitRanges&               split_ranges,
               int                              num_groups,
               int                              num_metrics,
               bool                             only_binary_metrics,
               Shard::packed_table_ptr          sample_table,
               const std::vector<int>&          socket_fds,
               const TermProviders<IntTerm>&    int_providers,
               const TermProviders<StringTerm>& str_providers)
            : _id(id) {
            worker_init(&_worker, id, num_groups, num_metrics, socket_fds.data(), socket_fds.size());
            session_init(&_session, num_groups, num_metrics, only_binary_metrics, sample_table);

            const SplitRanges::Range splits(split_ranges.splits_for(id));
            for (size_t split(splits.first); split <= splits.second; ++split) {
                _task_iterators.emplace_back(TaskIterator(&_worker, &_session, split, id,
                                                          int_providers, str_providers));
            }
        }

        Worker(const Worker& worker) = delete;

        ~Worker() {
            worker_destroy(&_worker);
            session_destroy(&_session);
        }

        size_t id() const { return _id; }

        void run() {
            static const TaskIterator task_it_end;
            bool done(false);
            while (!done) {
                done = true;
                for (std::vector<TaskIterator>::iterator it(_task_iterators.begin());
                     it != _task_iterators.end(); ++it) {
                    TaskIterator& task_it(*it);
                    if (task_it != task_it_end) {
                        const int err_result(*task_it);
                        if (err_result != 0) {
                            throw imhotep_error(std::string(_worker.error.str));
                        }
                        ++task_it;
                        done = false;
                    }
                }
            }
        }

    private:
        size_t _id;

        struct worker_desc        _worker;
        struct session_desc       _session;
        std::vector<TaskIterator> _task_iterators;
    };

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
        , _executor(executor)
    { }

    void FTGSRunner::run(int                     num_groups,
                         int                     num_metrics,
                         bool                    only_binary_metrics,
                         Shard::packed_table_ptr sample_table,
                         const std::vector<int>& socket_fds) {
        ProfilerStart("/tmp/googleymoogley.prof");

        const SplitRanges split_ranges(_num_splits, _num_workers);
        std::vector<std::unique_ptr<Worker>> workers;
        for (size_t id(0); id < _num_workers; ++id) {
            workers.emplace_back(new Worker(id, split_ranges,
                                            // num_groups, num_metrics, only_binary_metrics,
                                            num_groups, num_metrics, false,
                                            // !@# FIX ME!
                                            sample_table, socket_fds,
                                            _int_term_providers, _string_term_providers));
            Worker* worker(workers.back().get());
            _executor.enqueue([worker]() { worker->run(); });
        }
        _executor.await_completion();

        ProfilerStop();
    }

} // namespace imhotep
