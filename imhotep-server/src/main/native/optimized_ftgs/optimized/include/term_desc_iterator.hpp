#ifndef TERM_DESC_ITERATOR_HPP
#define TERM_DESC_ITERATOR_HPP

#include "merge_iterator.hpp"
#include "term_desc.hpp"

namespace imhotep {

    template <typename term_t>
    class TermDescIterator {
    public:
        typedef TermDesc value_type;
        typedef MergeIterator<term_t> iterator_t;

        TermDescIterator() { }

        TermDescIterator(const iterator_t begin, const iterator_t end)
            : _begin(begin)
            , _end(end)
        { }

        bool hasNext(void) const;

        void next(value_type& data);

    private:
        iterator_t _begin;
        iterator_t _end;
    };

} // namespace imhotep

#endif
