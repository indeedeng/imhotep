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

        TGSOpIterator(const Operation<term_t>&       operation,
                      const TermSeqIterator<term_t>& begin,
                      const TermSeqIterator<term_t>& end);

    private:
        friend class boost::iterator_core_access;

        void increment();

        bool equal(const TGSOpIterator& other) const;

        const Operation<term_t>& dereference() const { return _operation; }

        Operation<term_t> _operation;

        TermSeqIterator<term_t> _current;
        TermSeqIterator<term_t> _end;
    };


    template <typename term_t>
    TGSOpIterator<term_t>::TGSOpIterator(const Operation<term_t>&       operation,
                                         const TermSeqIterator<term_t>& begin,
                                         const TermSeqIterator<term_t>& end)
        : _operation(operation)
        , _current(begin)
        , _end(end) {
        increment();
    }

    template <typename term_t>
    void TGSOpIterator<term_t>::increment() {
        if (_current != _end) {
            _operation = Operation<term_t>::tgs(_operation, *_current);
            ++_current;
        }
        else {
            _operation = Operation<term_t>();
        }
    }

    template <typename term_t>
    bool TGSOpIterator<term_t>::equal(const TGSOpIterator& other) const  {
        return _current   == other._current
            && _operation == other._operation;
    }

} // namespace imhotep

#endif
