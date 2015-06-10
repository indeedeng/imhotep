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
            for (auto kv: splitters.back().splits()) {
                const std::string filename(shard.split_filename(split_dir, kv.second, kv.first));
                _splits.insert(std::make_pair(kv.first, SplitDesc(filename, shard)));
            }
        }
        for (Splitter<term_t>& splitter: splitters) {
            executor.enqueue([&splitter]() { splitter.run(); } );;
        }
        executor.await_completion();
    }

    template <typename term_t>
    TermSeqIterator<term_t> TermProvider<term_t>::term_seq_it(size_t split) const {
        typedef split_map_t::const_iterator map_it_t;

        std::vector<MergeInput<term_t>> merge_inputs;

        std::pair<map_it_t, map_it_t> matches(splits().equal_range(split));
        std::transform(matches.first, matches.second, std::back_inserter(merge_inputs),
                       [](std::pair<size_t, const SplitDesc&> entry) {
                           const SplitDesc& split_desc(entry.second);
                           return MergeInput<term_t>(SplitIterator<term_t>(split_desc.view()),
                                                     split_desc.table(),
                                                     nullptr); // !@# return real pointer!!!!!
                       });

        MergeIterator<term_t> begin(merge_inputs.begin(), merge_inputs.end());
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

    template TermSeqIterator<IntTerm>    TermProvider<IntTerm>::term_seq_it(size_t split)    const;
    template TermSeqIterator<StringTerm> TermProvider<StringTerm>::term_seq_it(size_t split) const;

} // namespace imhotep
