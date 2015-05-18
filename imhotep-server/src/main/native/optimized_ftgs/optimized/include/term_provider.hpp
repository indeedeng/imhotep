#ifndef TERM_PROVIDER_HPP
#define TERM_PROVIDER_HPP

#include <map>
#include <string>
#include <thread>
#include <vector>

#include "merge_iterator.hpp"
#include "splitter.hpp"

namespace imhotep {

    template <typename term_t>
    class TermProvider {
    public:
        typedef std::multimap<size_t, std::string> split_map_t;

        TermProvider()                        = delete;
        TermProvider(const TermProvider& rhs) = delete;

        TermProvider(const std::vector<std::string>& shards,
                     const std::string& field,
                     const std::string& split_dir,
                     size_t num_splits);

        merge_iterator<term_t> begin(size_t split);
        merge_iterator<term_t> end(size_t split);

        const split_map_t& splits() const { return _splits; }

    private:
        split_map_t _splits;
    };


    template <typename term_t>
    TermProvider<term_t>::TermProvider(const std::vector<std::string>& shards,
                                       const std::string& field,
                                       const std::string& split_dir,
                                       size_t num_splits)
    {
        std::vector<std::thread> threads;
        std::vector<Splitter<term_t>> splitters;
        for (std::string shard: shards) {
            splitters.push_back(Splitter<term_t>(shard, field, split_dir, num_splits));
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

} // namespace imhotep

#endif
