#ifndef TGS_OP_ITERATOR_HPP
#define TGS_OP_ITERATOR_HPP

#include <boost/iterator/iterator_facade.hpp>

#include "operation.hpp"
#include "term_seq_iterator.hpp"

namespace imhotep {

    template <typename term_t>
    class TGSOpIterator
        : public boost::iterator_facade<TGSOpIterator<term_t>,
                                        Operation<term_t> const,
                                        boost::forward_traversal_tag> {
    public:
        typedef TermSeqIterator<term_t> term_seq_it;

        TGSOpIterator() { }

        TGSOpIterator(const Operation<term_t>& operation, term_seq_it begin, term_seq_it end)
            : _operation(operation)
            , _current(begin)
            , _end(end) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        void increment() {
            if (_current == _end) return;

            _operation = Operation<term_t>::tgs(_operation, *_current);
            ++_current;
        }

        bool equal(const TGSOpIterator& other) const  {
            return _current == other._current;
        }

        const Operation<term_t>& dereference() const {
            return _operation;
        }

        Operation<term_t> _operation;

        term_seq_it _current;
        term_seq_it _end;
    };

} // namespace imhotep

#endif
