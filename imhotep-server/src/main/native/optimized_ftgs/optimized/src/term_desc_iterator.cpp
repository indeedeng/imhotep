#include "term_desc_iterator.hpp"

namespace imhotep {

    size_t TermDesc::count() const
    {
        assert(_docid_addresses.size() == _doc_freqs.size());
        assert(_docid_addresses.size() == _tables.size());
        return _doc_freqs.size();
    }

    void TermDesc::reset(const int64_t id)
    {
        _int_term = id;
        _is_int_type = true;
        _string_term.clear();
        _docid_addresses.clear();
        _doc_freqs.clear();
        _tables.clear();
    }

    void TermDesc::reset(const std::string& id)
    {
        _int_term = -1;
        _string_term = id;
        _is_int_type = false;
        _docid_addresses.clear();
        _doc_freqs.clear();
        _tables.clear();
    }

    template<>
    inline TermDesc& TermDesc::append<IntTerm>(const IntTerm& term,
                                               Shard::packed_table_ptr table)
    {
        assert(_int_term == term.id());
        _docid_addresses.push_back(term.doc_offset());
        _doc_freqs.push_back(term.doc_freq());
        _tables.push_back(table);
        return *this;
    }

    template<>
    inline TermDesc& TermDesc::append<StringTerm>(const StringTerm& term,
                                                  Shard::packed_table_ptr table)
    {
        assert(_string_term == term.id());
        _docid_addresses.push_back(term.doc_offset());
        _doc_freqs.push_back(term.doc_freq());
        _tables.push_back(table);
        return *this;
    }

    template <typename term_t>
    void TermDescIterator<term_t>::next(value_type& data)
    {
        if (_begin == _end) {
            data = TermDesc();
            return;
        }

        data.reset(_begin->first.id());
        const typename term_t::id_t current_id(_begin->first.id());
        while (_begin != _end && _begin->first.id() == current_id) {
            data.append((*_begin).first, (*_begin).second);
            ++_begin;
        }
    }

    template <typename term_t>
    bool TermDescIterator<term_t>::hasNext(void) const
    {
        return _begin != _end;
    }

    template void TermDescIterator<IntTerm>::next(TermDesc& data);
    template void TermDescIterator<StringTerm>::next(TermDesc& data);

    template bool TermDescIterator<IntTerm>::hasNext(void) const;
    template bool TermDescIterator<StringTerm>::hasNext(void) const;

} // namespace imhotep
