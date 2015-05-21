#ifndef TERM_PROVIDER_HPP
#define TERM_PROVIDER_HPP

#include <algorithm>
#include <iterator>
#include <map>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "merge_iterator.hpp"
#include "split_iterator.hpp"
#include "splitter.hpp"
#include "term_desc_iterator.hpp"

namespace imhotep {

    template <typename term_t>
    class TermProvider {
    public:
        typedef std::pair<std::string, TermIterator<term_t>> term_source_t;
        typedef std::multimap<size_t, std::string>           split_map_t;

        TermProvider()                    = delete;
        TermProvider(const TermProvider&) = default;

        TermProvider& operator=(const TermProvider&) = default;

        TermProvider(const std::vector<term_source_t>& sources,
                     const std::string&                field,
                     const std::string&                split_dir,
                     size_t                            num_splits);

        TermDescIterator<MergeIterator<term_t>> merge(size_t split) const;

        const split_map_t& splits() const { return _splits; }

    private:
        split_map_t _splits;
    };

    typedef TermProvider<IntTerm>    IntTermProvider;
    typedef TermProvider<StringTerm> StringTermProvider;

    /*

    template <typename term_t>
    TermProvider<term_t>::TermProvider(const std::vector<term_source_t>& sources,
                                       const std::string&                field,
                                       const std::string&                split_dir,
                                       size_t                            num_splits)
    {
        std::vector<std::thread> threads;
        std::vector<Splitter<term_t>> splitters;
        for (term_source_t source: sources) {
            const std::string&   shardname(source.first);
            TermIterator<term_t> term_iterator(source.second);
            splitters.push_back(Splitter<term_t>(shardname, field, term_iterator,
                                                 split_dir, num_splits));
            const std::vector<std::string>& splits(splitters.back().splits());
            for (size_t split_num(0); split_num < splits.size(); ++split_num) {
                _splits.insert(std::make_pair(split_num, splits[split_num]));
            }
        }
        for (Splitter<term_t>& splitter: splitters) {
            threads.push_back(std::thread([&splitter]() { splitter.run(); } ));
        }
        for (std::thread& th: threads) {
            th.join();
        }
    }
    */

    template <typename term_t>
    TermDescIterator<MergeIterator<term_t>> TermProvider<term_t>::merge(size_t split) const
    {
        typedef split_map_t::const_iterator map_it_t;
        std::vector<SplitIterator<term_t>> split_its;
        std::pair<map_it_t, map_it_t> matches(splits().equal_range(split));
        std::transform(matches.first, matches.second, std::back_inserter(split_its),
                       [](std::pair<size_t, const std::string&> entry) {
                           return SplitIterator<term_t>(entry.second);
                       });
        MergeIterator<term_t> begin(split_its.begin(), split_its.end());
        MergeIterator<term_t> end;
        return TermDescIterator<MergeIterator<term_t>>(begin, end);
    }

} // namespace imhotep

#endif
