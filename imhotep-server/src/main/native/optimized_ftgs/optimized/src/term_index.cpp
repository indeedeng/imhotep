#include "term_index.hpp"

#include "string_range.hpp"

namespace imhotep {

    TermIndex::TermIndex(const char*       begin,
                         const char*       end,
                         const VarIntView& term_view)
        : _begin(begin)
        , _end(end)
        , _term_view(term_view)
    { }


    IntTermIndex::IntTermIndex(const char*       begin,
                               const char*       end,
                               const VarIntView& term_view)
        : TermIndex(begin, end, term_view)
        , _root(begin + header().root_index_start_address())
    { }

    TermIterator<IntTerm> IntTermIndex::find_it(int64_t key) const {
        const Result result(find(key));
        if (!result.is_nil()) {
            const VarIntView view(term_view().begin() + result.term_offset());
            return TermIterator<IntTerm>(view, key, result.doc_offset());
        }
        return TermIterator<IntTerm>();
    }

    TermIndex::Result IntTermIndex::find(int64_t key) const {
        using btree::KeyValue;
        using btree::LongPair;
        typedef btree::Int<int64_t> key_t;

        const key_t  btree_key(reinterpret_cast<const char*>(&key));
        const size_t level(header().index_levels());
        const KeyValue<key_t, LongPair> result(_root.find<LongPair>(begin(), btree_key, level));
        return result.is_nil() ? Result() : Result(result.value());
    }


    StringTermIndex::StringTermIndex(const char*       begin,
                                     const char*       end,
                                     const VarIntView& term_view)
        : TermIndex(begin, end, term_view)
        , _root(begin + header().root_index_start_address())
    { }

    TermIterator<StringTerm> StringTermIndex::find_it(const std::string& key) const {
        const Result result(find(key));
        if (!result.is_nil()) {
            const VarIntView view(term_view().begin() + result.term_offset());
            return TermIterator<StringTerm>(view, StringRange(key), result.doc_offset());
        }
        return TermIterator<StringTerm>();
    }

    TermIndex::Result StringTermIndex::find(const std::string& key) const {
        using btree::KeyValue;
        using btree::LongPair;
        using btree::String;

        _buffer.clear();
        String::encode(key);
        const String btree_key(_buffer.data());
        const size_t level(header().index_levels());
        KeyValue<String, LongPair> result(_root.find<LongPair>(begin(), btree_key, level));
        return result.is_nil() ? Result() : Result(result.value());
    }

} // namespace imhotep
