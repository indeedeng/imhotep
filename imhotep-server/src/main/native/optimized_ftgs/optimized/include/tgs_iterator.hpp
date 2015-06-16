#ifndef OPERATION_ITERATOR_HPP
#define OPERATION_ITERATOR_HPP

#include "merge_iterator.hpp"
#include "operation.hpp"
#include "term_desc.hpp"

namespace imhotep {

    template <typename term_t>
    class OperationIterator
        : public boost::iterator_facade<OperationIterator<term_t>,
                                        Operation const,
                                        boost::forward_traversal_tag> {
    public:
        typedef MergeIterator<term_t> merge_it;

        OperationIterator() { }

        OperationIterator(merge_it begin, merge_it end)
            : _current(begin)
            , _end(end)
        { }

    private:
        Operation _operation;

        merge_it _current;
        merge_it _end;
    };

} // namespace imhotep

#endif
