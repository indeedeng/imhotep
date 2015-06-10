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
#include "term_desc_iterator.hpp"
#include "term_seq_iterator.hpp"

namespace imhotep {

    class SplitDesc {
    public:
        SplitDesc(const std::string& filename, Shard::packed_table_ptr table)
            : _filename(filename)
            , _table(table)
        { }

        const std::string& filename() const { return _filename; }

        const Shard::packed_table_ptr table() const { return _table; }

        SplitView view() const {
            if (!_file) _file.reset(new MMappedFile(filename(), true));
            return SplitView(_file->begin(), _file->end());
        }

    private:
        const std::string             _filename;
        const Shard::packed_table_ptr _table;

        mutable std::shared_ptr<MMappedFile> _file;
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

        TermSeqIterator<term_t> term_seq_it(size_t split) const;

        const split_map_t& splits() const { return _splits; }

    private:
        split_map_t _splits;
    };

    typedef TermProvider<IntTerm>    IntTermProvider;
    typedef TermProvider<StringTerm> StringTermProvider;

} // namespace imhotep

#endif
