#include "term_provider.hpp"

#include "split_iterator.hpp"

namespace imhotep {

    template <typename term_t>
    TermProvider<term_t>::TermProvider(const std::vector<term_source_t>& sources,
                                       const std::string&                field,
                                       const std::string&                split_dir,
                                       size_t                            num_splits,
                                       ExecutorService&                  executor) {
        std::vector<Splitter<term_t>> splitters;
        for (term_source_t source: sources) {
            const Shard&         shard(source.first);
            TermIterator<term_t> term_iterator(source.second);
            splitters.push_back(Splitter<term_t>(shard, field, term_iterator,
                                                 split_dir, num_splits));
            const std::vector<std::string>& splits(splitters.back().splits());
            for (size_t split_num(0); split_num < splits.size(); ++split_num) {
                _splits.insert(std::make_pair(split_num, SplitDesc(splits[split_num], shard.table())));
            }
        }
        for (Splitter<term_t>& splitter: splitters) {
            executor.enqueue([&splitter]() { splitter.run(); } );;
        }
        executor.await_completion();
    }

    /******* Delete me! *******/
    template <typename term_t>
    TermDescIterator<term_t> TermProvider<term_t>::merge(size_t split) const {
        typedef split_map_t::const_iterator map_it_t;

        std::vector<typename MergeIterator<term_t>::Entry> pairs;

        std::pair<map_it_t, map_it_t> matches(splits().equal_range(split));
        std::transform(matches.first, matches.second, std::back_inserter(pairs),
                       [](std::pair<size_t, const SplitDesc&> entry) {
                           const SplitDesc& split_desc(entry.second);
                           return std::make_pair(SplitIterator<term_t>(split_desc.view()),
                                                 split_desc.table());
                       });

        MergeIterator<term_t> begin(pairs.begin(), pairs.end());
        MergeIterator<term_t> end;

        return TermDescIterator<term_t>(begin, end);
    }
    /******* Delete me! *******/

    template <typename term_t>
    TermSeqIterator<term_t> TermProvider<term_t>::term_seq_it(size_t split) const {
        typedef split_map_t::const_iterator map_it_t;

        std::vector<typename MergeIterator<term_t>::Entry> pairs;

        std::pair<map_it_t, map_it_t> matches(splits().equal_range(split));
        std::transform(matches.first, matches.second, std::back_inserter(pairs),
                       [](std::pair<size_t, const SplitDesc&> entry) {
                           const SplitDesc& split_desc(entry.second);
                           return std::make_pair(SplitIterator<term_t>(split_desc.view()),
                                                 split_desc.table());
                       });

        MergeIterator<term_t> begin(pairs.begin(), pairs.end());
        MergeIterator<term_t> end;

        return TermSeqIterator<term_t>(begin, end);
    }


    /* template instantiations */

    template
    TermProvider<IntTerm>::TermProvider(const std::vector<term_source_t>& sources,
                                        const std::string&                field,
                                        const std::string&                split_dir,
                                        size_t                            num_splits,
                                        ExecutorService&                  executor);

    template
    TermProvider<StringTerm>::TermProvider(const std::vector<term_source_t>& sources,
                                           const std::string&                field,
                                           const std::string&                split_dir,
                                           size_t                            num_splits,
                                           ExecutorService&                  executor);


    template TermDescIterator<IntTerm>    TermProvider<IntTerm>::merge(size_t split)    const;
    template TermDescIterator<StringTerm> TermProvider<StringTerm>::merge(size_t split) const;

} // namespace imhotep
