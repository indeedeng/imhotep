#ifndef TERM_INDEX_HPP
#define TERM_INDEX_HPP

#include <string>
#include <utility>
#include <vector>

#include "btree/header.hpp"
#include "btree/key_value.hpp"
#include "btree/index_block.hpp"
#include "btree/string.hpp"
#include "term_iterator.hpp"
#include "var_int_view.hpp"

namespace imhotep {

    class TermIndex {
    public:
        TermIndex(const char*       begin,
                  const char*       end,
                  const VarIntView& term_view);

        btree::Header header() const { return btree::Header(end() - btree::Header::length()); }

    protected:
        class Result : public std::pair<int64_t, int64_t> {
        public:
            Result() : std::pair<int64_t, int64_t>(-1, -1) { }

            Result(const btree::LongPair& value)
                : std::pair<int64_t, int64_t>(value().first, value().second)
            { }

            bool is_nil() const { return first < 0 || second < 0; }

            size_t term_offset() const { return first;  }
            size_t  doc_offset() const { return second; }
        };

        const char* begin() const { return _begin; }
        const char*   end() const { return _end;   }

        const VarIntView&  term_view() const { return _term_view;  }

    private:
        const char*      _begin;
        const char*      _end;
        const VarIntView _term_view;
    };

    class IntTermIndex : public TermIndex {
    public:
        IntTermIndex(const char*       begin,
                     const char*       end,
                     const VarIntView& term_view);

        TermIterator<IntTerm> find_it(int64_t key) const;

        Result find(int64_t key) const;

    private:
        const btree::IndexBlock<btree::Int<int64_t>> _root;
    };

    class StringTermIndex : public TermIndex {
    public:
        StringTermIndex(const char*       begin,
                        const char*       end,
                        const VarIntView& term_view);

        TermIterator<StringTerm> find_it(const std::string& key) const;

        Result find(const std::string& key) const;

    private:
        const btree::IndexBlock<btree::String> _root;
        mutable std::vector<char>              _buffer;
    };

} // namespace imhotep

#endif
