#ifndef TERM_INDEX_HPP
#define TERM_INDEX_HPP

#include <iostream>
#include <string>
#include <vector>

#include "btree/header.hpp"
#include "btree/key_value.hpp"
#include "btree/index_block.hpp"
#include "btree/string.hpp"
#include "term_iterator.hpp"
#include "var_int_view.hpp"

namespace imhotep {

    template <typename term_t>
    struct BufferTraits {
        typedef void buffer_t;
    };

    template <>
    struct BufferTraits<IntTerm> {
        typedef IntTerm::id_t buffer_t;
    };

    template <>
    struct BufferTraits<StringTerm> {
        typedef std::string buffer_t;
    };

    class TermIndex {
    public:
        TermIndex(const char*       begin,
                  const char*       end,
                  const VarIntView& term_view);

        btree::Header header() const { return btree::Header(end() - btree::Header::length()); }

        template <typename term_t>
        class Result {
        public:
            typedef typename BufferTraits<term_t>::buffer_t term_id_t;

            Result()
                : _term_id(term_id_t())
                , _term_offset(-1)
                , _doc_offset(-1)
            { }

            Result(const term_id_t& term_id, const btree::LongPair& value)
                : _term_id(term_id)
                , _term_offset(value().first)
                , _doc_offset(value().second)
            { }

            bool is_nil() const { return term_offset() < 0 || doc_offset() < 0; }

            term_id_t term_id() const { return _term_id; }

            size_t term_offset() const { return _term_offset; }
            size_t  doc_offset() const { return _doc_offset;  }

        private:
            const term_id_t _term_id;
            const int64_t   _term_offset;
            const int64_t   _doc_offset;
        };

        const char* begin() const { return _begin; }
        const char*   end() const { return _end;   }

        const VarIntView&  term_view() const { return _term_view;  }

    private:
        const char*      _begin;
        const char*      _end;
        const VarIntView _term_view;
    };

    /* !@# TODO(johnf) combine IntTerm with StringTerm version? Can use term
        traits to selectively include _buffer ala term iterator. */

    class IntTermIndex : public TermIndex {
    public:
        IntTermIndex(const char*       begin,
                     const char*       end,
                     const VarIntView& term_view);

        /** Find an item and return a term iterator positioned to it. Return an
            end iterator if it's not found. */
        TermIterator<IntTerm> find_it(int64_t key) const;

        /** Find an item or return a nil Result. */
        Result<IntTerm> find(int64_t key) const;

    private:
        const btree::IndexBlock<btree::Int<int64_t>> _root;
    };

    class StringTermIndex : public TermIndex {
    public:
        StringTermIndex(const char*       begin,
                        const char*       end,
                        const VarIntView& term_view);

        /** Find an item and return a term iterator positioned to it. Return an
            end iterator if it's not found. */
        TermIterator<StringTerm> find_it(const std::string& key) const;

        /** Find an item or return a nil Result. */
        Result<StringTerm> find(const std::string& key) const;

    private:
        const btree::IndexBlock<btree::String> _root;

        mutable std::vector<char> _buffer; // !@# comment
    };

} // namespace imhotep

template <typename term_t>
std::ostream& operator<<(std::ostream& os, const imhotep::TermIndex::Result<term_t>& result) {
    if (result.is_nil()) {
        os << "(nil Result)";
    }
    else {
        os << "Result {"
           << " term_offset: " << result.term_offset()
           << " doc_offset: "  << result.doc_offset()
           << "}";
    }
    return os;
}

#endif
