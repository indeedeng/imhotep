#ifndef TERM_ITERATOR_HPP
#define TERM_ITERATOR_HPP

#include <cassert>
#include <cstring>
#include <memory>
#include <cstdint>
#include <string>

#include <boost/iterator/iterator_facade.hpp>

#include "shard.hpp"
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

        TermIterator(const Shard&       shard,
                     const std::string& field)
            : _term_view(shard.term_view<term_t>(field))
            , _docid_view(shard.docid_view<term_t>(field))
            , _docid_base(_docid_view ? reinterpret_cast<int64_t>(_docid_view->begin()) : 0) {
            increment();
        }

        TermIterator(const char* begin, const char* end)
            : _term_view(std::make_shared<VarIntView>(begin, end)) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        void increment();

        bool equal(const TermIterator& other) const { return *_term_view == *other._term_view; }

        const term_t& dereference() const { return _current; }

        std::shared_ptr<VarIntView> _term_view  = std::make_shared<VarIntView>();
        std::shared_ptr<VarIntView> _docid_view = std::make_shared<VarIntView>();

        const int64_t _docid_base = 0;

        term_t _current;

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
