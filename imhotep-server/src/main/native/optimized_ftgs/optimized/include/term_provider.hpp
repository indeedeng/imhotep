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

} // namespace imhotep

#endif
