#include "term_desc_iterator.hpp"

namespace imhotep {

    inline
    size_t TermDesc::count() const
    {
        assert(_docid_addresses.size() == _doc_freqs.size());
        return _doc_freqs.size();
    }

    inline
    void TermDesc::reset(const int64_t id, Shard::packed_table_ptr table)
    {
        _int_term = id;
        _is_int_type = true;
        _string_term.clear();
        _docid_addresses.clear();
        _doc_freqs.clear();
        _table = table;
    }

    inline
    void TermDesc::reset(const std::string& id, Shard::packed_table_ptr table)
    {
        _int_term = -1;
        _string_term = id;
        _is_int_type = false;
        _docid_addresses.clear();
        _doc_freqs.clear();
        _table = table;
    }

    template<>
    inline TermDesc& TermDesc::operator+=<IntTerm>(const IntTerm& term)
    {
        assert(_int_term == term.id());
        _docid_addresses.push_back(term.doc_offset());
        _doc_freqs.push_back(term.doc_freq());
        return *this;
    }

    template<>
    inline TermDesc& TermDesc::operator+=<StringTerm>(const StringTerm& term)
    {
        assert(_string_term == term.id());
        _docid_addresses.push_back(term.doc_offset());
        _doc_freqs.push_back(term.doc_freq());
        return *this;
    }

    template <typename term_t>
    void TermDescIterator<term_t>::next(value_type& data)
    {
        if (_begin == _end) {
            data = TermDesc();
            return;
        }

        data.reset(_begin->first.id(), _begin->second);
        const typename term_t::id_t current_id(_begin->first.id());
        while (_begin != _end && _begin->first.id() == current_id) {
            data += (*_begin).first;
            ++_begin;
        }
    }

    template <typename term_t>
    const bool TermDescIterator<term_t>::hasNext(void) const
    {
        return _begin != _end;
    }

} // namespace imhotep
