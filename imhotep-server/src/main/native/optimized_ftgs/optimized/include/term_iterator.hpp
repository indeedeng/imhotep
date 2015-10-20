#ifndef TERM_ITERATOR_HPP
#define TERM_ITERATOR_HPP

#include <cstring>
#include <memory>
#include <cstdint>
#include <string>

#include <boost/iterator/iterator_facade.hpp>

#include "term.hpp"
#include "var_int_view.hpp"

namespace imhotep {

    template <typename term_t>
    struct TermIteratorTraits {
        typedef void buffer_t;
    };

    template <typename term_t>
    class TermIterator
        : public boost::iterator_facade<TermIterator<term_t>, term_t const,
                                        boost::forward_traversal_tag> {
    public:
        TermIterator() { }

        TermIterator(const VarIntView& term_view)
            : _term_view(term_view) {
            increment();
        }

        TermIterator(const VarIntView&            term_view,
                     const typename term_t::id_t& term_id,
                     int64_t                      doc_offset);

    private:
        friend class boost::iterator_core_access;

        void increment();

        bool equal(const TermIterator& other) const {
            return _current == other._current;
        }

        const term_t& dereference() const { return _current; }

        VarIntView _term_view;
        term_t     _current;

        typename TermIteratorTraits<term_t>::buffer_t _id_buffer;
    };

    template <> struct TermIteratorTraits<IntTerm> {
        struct Unused { };
        typedef Unused buffer_t;
    };

    template <> struct TermIteratorTraits<StringTerm> {
        typedef std::string buffer_t;
    };

    typedef TermIterator<IntTerm>    IntTermIterator;
    typedef TermIterator<StringTerm> StringTermIterator;

} // namespace imhotep

#endif
