#include "term_provider.hpp"

#include "split_iterator.hpp"

namespace imhotep {

    template <typename term_t>
    TermProvider<term_t>::TermProvider(const std::vector<term_source_t>& sources,
                                       const std::string&                field,
                                       const std::string&                split_dir,
                                       size_t                            num_splits,
                                       ExecutorService&                  executor)
        : _split_dir(split_dir) {

        std::vector<Splitter<term_t>> splitters;

        for (typename std::vector<term_source_t>::const_iterator it(sources.begin());
             it != sources.end(); ++it) {
            const term_source_t& source(*it);
            const Shard*         shard(source.first);
            TermIterator<term_t> term_iterator(source.second);
            splitters.push_back(Splitter<term_t>(shard, field, term_iterator,
                                                 _split_dir, num_splits));

            const typename Splitter<term_t>::SplitNumToField& smtof(splitters.back().splits());
            for (typename Splitter<term_t>::SplitNumToField::const_iterator split_it(smtof.begin());
                 split_it != smtof.end(); ++split_it) {
                const std::pair<size_t, std::string>& kv(*split_it);
                _splits.insert(std::make_pair(kv.first, SplitDesc(kv.first, kv.second, shard)));
            }
        }

        for (typename std::vector<Splitter<term_t>>::iterator it(splitters.begin());
             it != splitters.end(); ++it) {
            Splitter<term_t>& splitter(*it);
            executor.enqueue(std::bind(&Splitter<term_t>::run, splitter));
        }
        executor.await_completion();
    }

    template <typename term_t>
    MergeIterator<term_t> TermProvider<term_t>::merge_it(size_t split) const {
        typedef split_map_t::const_iterator map_it_t;

        std::vector<MergeInput<term_t>> merge_inputs;

        std::pair<map_it_t, map_it_t> matches(splits().equal_range(split));

        for (map_it_t it(matches.first); it != matches.second; ++it) {
            const std::pair<size_t, const SplitDesc&>& entry(*it);
            const SplitDesc&   split_desc(entry.second);
            const std::string& field(split_desc.field());
            VarIntView         docid_view(split_desc.shard().docid_view<term_t>(field));
            merge_inputs.emplace_back(MergeInput<term_t>(SplitIterator<term_t>(split_desc.view(_split_dir)),
                                                         split_desc.table(), docid_view.begin()));
        }
        return MergeIterator<term_t>(merge_inputs.begin(), merge_inputs.end());
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

    template MergeIterator<IntTerm>    TermProvider<IntTerm>::merge_it(size_t split)    const;
    template MergeIterator<StringTerm> TermProvider<StringTerm>::merge_it(size_t split) const;

} // namespace imhotep
