#ifndef TGS_OP_ITERATOR_HPP
#define TGS_OP_ITERATOR_HPP

#include "merge_iterator.hpp"
#include "operation.hpp"
#include "term_desc.hpp"

namespace imhotep {

    template <typename term_t>
    class TGSOpIterator
        : public boost::iterator_facade<TGSOpIterator<term_t>,
                                        Operation const,
                                        boost::forward_traversal_tag> {
    public:
        typedef MergeIterator<term_t> merge_it;

        TGSOpIterator() { }

        TGSOpIterator(const Operation operation, merge_it begin, merge_it end)
            : _operation(operation)
            , _current(begin)
            , _end(end) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        void increment() {
            if (_current == _end) return;

            _operation = Operation(_operation, *_current);
            ++_current;
        }

        bool equel(const Operation& other) const  {
            return _operation == other._operation; // !@# sufficient?
        }

        const Operation& dereference() const {
            return _operation;
        }

        Operation _operation;

        merge_it _current;
        merge_it _end;
    };

} // namespace imhotep

#endif
