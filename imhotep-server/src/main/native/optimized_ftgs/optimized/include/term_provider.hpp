#ifndef TERM_PROVIDER_HPP
#define TERM_PROVIDER_HPP

#include <algorithm>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "executor_service.hpp"
#include "merge_iterator.hpp"
#include "mmapped_file.hpp"
#include "shard.hpp"
#include "splitter.hpp"
#include "split_view.hpp"

namespace imhotep {

    class SplitDesc {
    public:
        SplitDesc(size_t split_num, const std::string& field, const Shard& shard)
            : _split_num(split_num)
            , _field(field)
            , _shard(shard)
        { }

        size_t split_num() const { return _split_num; }

        const std::string& field() const { return _field; }

        const Shard& shard() const { return _shard; }

        const Shard::packed_table_ptr table() const { return _shard.table(); }

        SplitView view(const std::string& split_dir) const {
            return _shard.split_view(_shard.split_filename(split_dir, _field, _split_num));
        }

    private:
        const size_t      _split_num;
        const std::string _field;
        const Shard       _shard;
    };


    template <typename term_t>
    class TermProvider {
    public:
        typedef std::pair<Shard, TermIterator<term_t>> term_source_t;
        typedef std::multimap<size_t, SplitDesc>       split_map_t;

        TermProvider()                    = delete;
        TermProvider(const TermProvider&) = default;

        TermProvider& operator=(const TermProvider&) = default;

        TermProvider(const std::vector<term_source_t>& sources,
                     const std::string&                field,
                     const std::string&                split_dir,
                     size_t                            num_splits,
                     ExecutorService&                  executor);

        MergeIterator<term_t> merge_it(size_t split) const;

        const split_map_t& splits() const { return _splits; }

    private:
        std::string _split_dir;
        split_map_t _splits;
    };

    typedef TermProvider<IntTerm>    IntTermProvider;
    typedef TermProvider<StringTerm> StringTermProvider;

} // namespace imhotep

#endif
