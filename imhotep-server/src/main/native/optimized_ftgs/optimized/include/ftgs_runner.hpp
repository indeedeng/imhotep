#ifndef FTGS_RUNNER_HPP
#define FTGS_RUNNER_HPP

#include <algorithm>
#include <iterator>
#include <utility>
#include <vector>

#include "jiterator.hpp"
#include "chained_iterator.hpp"
#include "executor_service.hpp"
#include "shard.hpp"
#include "term_provider.hpp"
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

        TermDesc& operator()(op_desc& desc) {
            return _termDesc;
        }

        const TermDesc& term_desc() const { return _termDesc; }

        int32_t split_index() const { return _splitIndex; }

        int8_t operation() const { return _operation; }

        const std::string field_name() const { return _fieldName; }

        Encoding term_type() const { return _termDesc.encode_type(); }

        int int_term() const { return _termDesc.int_term(); }

        const char* str_term() const {
            return term_type() == INT_TERM_TYPE ?
                nullptr : _termDesc.string_term().c_str();
        }

        int str_term_length() const {
            return term_type() == INT_TERM_TYPE ?
                0 : _termDesc.string_term().length();
        }
    private:
        TermDesc     _termDesc;
        int32_t      _splitIndex;
        int8_t       _operation;
        std::string  _fieldName;
    };


    template <typename term_t>
    class TermProviders : public std::vector<std::pair<std::string, TermProvider<term_t>>> {
    public:
        TermProviders(const std::vector<Shard>&       shards,
                      const std::vector<std::string>& field_names,
                      const std::string&              split_dir,
                      size_t                          num_splits,
                      ExecutorService&                executor);

    private:
        typedef TermIterator<term_t>      term_it;
        typedef std::pair<Shard, term_it> term_source_t;

        std::vector<term_source_t> term_sources(const std::vector<Shard>& shards,
                                                const std::string&        field) const {
            std::vector<term_source_t> result;
            std::transform(shards.begin(), shards.end(),
                           std::back_inserter(result),
                           [&field](const Shard& shard) {
                               return std::make_pair(shard, term_it(shard, field));
                           });
            return result;
        }
    };

    class FTGSRunner {
    public:
        FTGSRunner(const std::vector<Shard>&       shards,
                   const std::vector<std::string>& int_fieldnames,
                   const std::vector<std::string>& string_fieldnames,
                   const std::string&              split_dir,
                   size_t                          num_splits,
                   size_t                          num_workers,
                   ExecutorService&                executor);

        FTGSRunner(const FTGSRunner& rhs) = delete;

        const std::vector<std::string>& getIntFieldnames() const
        {
            return _int_fieldnames;
        }

        const TermProviders<IntTerm>& getIntTermProviders() const
        {
            return _int_term_providers;
        }

        const size_t getNumSplits() const
        {
            return _num_splits;
        }

        const size_t getNumWorkers() const
        {
            return _num_workers;
        }

        const std::vector<std::string>& getStringFieldnames() const
        {
            return _string_fieldnames;
        }

        const TermProviders<StringTerm>& getStringTermProviders() const
        {
            return _string_term_providers;
        }

    private:
        template<typename term_t>
        auto forSplit_getFieldIters(const TermProviders<term_t>& providers, int split_num) ->
            std::vector<wrapping_jIterator<TermDescIterator<term_t>, op_desc, op_desc>>;

        template <typename iter1_t, typename iter2_t>
        auto forSplit_chainFieldIters(std::vector<iter1_t>& term_iter1,
                                      std::vector<iter2_t>& term_iter2,
                                      const int split_num)
            -> ChainedIterator<iter1_t, iter2_t>;

        auto forSplit_getMultiFieldTermDescIter(const int split_num)
        {
            auto int_field_iters = forSplit_getFieldIters(_int_term_providers, split_num);
            auto string_field_iters = forSplit_getFieldIters(_string_term_providers, split_num);

            return forSplit_chainFieldIters(int_field_iters, string_field_iters, split_num);
        }


        const std::vector<Shard>  _shards;
        const std::vector<std::string>& _int_fieldnames;
        const std::vector<std::string>& _string_fieldnames;
        const TermProviders<IntTerm>    _int_term_providers;
        const TermProviders<StringTerm> _string_term_providers;
        const size_t _num_splits;
        const size_t _num_workers;

    public:
        typedef uint32_t split_handle_t;

        std::vector<int> forWorker_getSplitNums(size_t worker_num)
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

        static int forWorker_getSplitOrdinal(split_handle_t handle, int split_num)
        {
            return split_num - handle;
        }

        auto build_worker(int worker_num,
                          int nGroups,
                          int nMetrics,
                          bool only_binary_metrics,
                          packed_table_t *sample_table,
                          int *socket_fds)
        {
            using int_transform = wrapping_jIterator<TermDescIterator<IntTerm>, op_desc, op_desc>;
            using string_transform = wrapping_jIterator<TermDescIterator<StringTerm>, op_desc, op_desc>;
            using chained_iter_t = ChainedIterator<int_transform,string_transform>;

            auto split_nums = forWorker_getSplitNums(worker_num);
            int nStreams = split_nums.size();
            int socket_file_descriptors[nStreams];
            std::vector<chained_iter_t> splits(nStreams);
            split_handle_t handle = split_nums[0];

            int i = 0;
            for (auto n : split_nums) {
                socket_file_descriptors[i] = n;
                splits.push_back(forSplit_getMultiFieldTermDescIter(n));
                i++;
            }

            struct worker_desc worker;
            worker_init(&worker, worker_num, nGroups, nMetrics, socket_file_descriptors, nStreams);
            struct session_desc session;
            session_init(&session, nGroups, nMetrics, only_binary_metrics, sample_table);

            auto iter = InterleavedJIterator<chained_iter_t>(splits.begin(), splits.end());

            return std::make_tuple(worker, session, iter, handle);
        }

    };

} // namespace imhotep

#endif
