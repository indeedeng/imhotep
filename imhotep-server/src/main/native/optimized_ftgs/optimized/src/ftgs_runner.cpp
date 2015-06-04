#define BOOST_RESULT_OF_USE_DECLTYPE    1

#include <tuple>
#include "ftgs_runner.hpp"

namespace imhotep {

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

    std::vector<int> FTGSRunner::forWorker_getSplitNums(size_t worker_num)
    {
        std::vector<int> results;
        size_t start;
        size_t end;
        const size_t num_workers = getNumWorkers();
        const size_t num_streams_per_worker = getNumSplits() / num_workers;
        const size_t num_plus_one_workers = getNumSplits() % num_workers;

        // find start and end of split range
        if (worker_num < num_plus_one_workers) {
            start = (num_streams_per_worker + 1) * worker_num;
            end = start + num_streams_per_worker + 1;
        } else if (worker_num < num_workers) {
            start = num_plus_one_workers * (num_streams_per_worker + 1)
                    + num_streams_per_worker * (worker_num - num_plus_one_workers);
            end = start + num_streams_per_worker;
        } else {
            // throw exception
        }

        // fill vector
        while (start < end) {
            results.push_back(start);
            start ++;
        }
        return results;
    }

    int FTGSRunner::forWorker_getSplitOrdinal(split_handle_t handle, int split_num)
    {
        return split_num - handle;
    }


} // namespace imhotep
