#include "term_provider.hpp"

namespace imhotep {

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

    template
    TermProvider<IntTerm>::TermProvider(const std::vector<term_source_t>& sources,
                                        const std::string&                field,
                                        const std::string&                split_dir,
                                        size_t                            num_splits);

    template
    TermProvider<StringTerm>::TermProvider(const std::vector<term_source_t>& sources,
                                           const std::string&                field,
                                           const std::string&                split_dir,
                                           size_t                            num_splits);


} // namespace imhotep
