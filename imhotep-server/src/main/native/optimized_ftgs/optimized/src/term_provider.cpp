#include "term_provider.hpp"

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
                _splits.insert(std::make_pair(split_num, splits[split_num]));
            }
        }
        for (Splitter<term_t>& splitter: splitters) {
            executor.enqueue([&splitter]() { splitter.run(); } );;
        }
        executor.await_completion();
    }

    template <typename term_t>
    TermDescIterator<MergeIterator<term_t>> TermProvider<term_t>::merge(size_t split) const {
        typedef split_map_t::const_iterator map_it_t;
        typedef SplitIterator<term_t>       split_it_t;

        std::vector<split_it_t> split_its;

        std::pair<map_it_t, map_it_t> matches(splits().equal_range(split));
        std::transform(matches.first, matches.second, std::back_inserter(split_its),
                       [](std::pair<size_t, const std::string&> entry) {
                           return SplitIterator<term_t>(entry.second);
                       });

        MergeIterator<term_t> begin(split_its.begin(), split_its.end());
        MergeIterator<term_t> end;

        return TermDescIterator<MergeIterator<term_t>>(begin, end);
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


    template TermDescIterator<MergeIterator<IntTerm>>    TermProvider<IntTerm>::merge(size_t split)    const;
    template TermDescIterator<MergeIterator<StringTerm>> TermProvider<StringTerm>::merge(size_t split) const;

} // namespace imhotep
