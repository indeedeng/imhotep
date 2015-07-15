#ifndef FIELD_OP_ITERATOR_HPP
#define FIELD_OP_ITERATOR_HPP

#include <sstream>

#include <boost/iterator/iterator_facade.hpp>

#include "merge_iterator.hpp"
#include "operation.hpp"
#include "term_providers.hpp"

namespace imhotep {

    /** Given a collection of TermProviders and a split number, FieldOpIterator
        produces a series of Operations for TaskIterator to execute. In other
        words, it implements the transitions depicted in the state transition
        diagram in operation.hpp.
     */
    template <typename term_t>
    class FieldOpIterator
        : public boost::iterator_facade<FieldOpIterator<term_t>,
                                        Operation<term_t> const,
                                        boost::forward_traversal_tag> {
    public:
        FieldOpIterator() : _split(0) { }

        FieldOpIterator(const TermProviders<term_t>& providers, size_t split);

    private:
        friend class boost::iterator_core_access;

        void reset_field();
        void increment();
        bool equal(const FieldOpIterator& other) const;
        const Operation<term_t>& dereference() const { return _operation; }

        typename TermProviders<term_t>::const_iterator _current;
        typename TermProviders<term_t>::const_iterator _end;

        size_t _split;

        Operation<term_t>     _operation;
        MergeIterator<term_t> _merge_current;
        MergeIterator<term_t> _merge_end;
    };

} // namespace imhotep

#endif
