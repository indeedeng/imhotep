#ifndef TERM_DESC_ITERATOR_HPP
#define TERM_DESC_ITERATOR_HPP

#include <cassert>
#include <iostream>
#include <vector>

#include "shard.hpp"

namespace imhotep {

    class TermDesc {
    public:
        const int64_t int_term() const { return _int_term; }
        const std::string& string_term() const { return _string_term; }

        const int64_t* docid_addresses() const { return _docid_addresses.data(); }
        const int64_t*       doc_freqs() const { return _doc_freqs.data();       }

        Shard::packed_table_ptr table() { return _table; }

        size_t count() const;

        void reset(const int64_t id, Shard::packed_table_ptr table);
        void reset(const std::string& id, Shard::packed_table_ptr table);

        template<typename id_type>
        TermDesc& operator+=(const Term<id_type>& term);

    private:
        int64_t              _int_term;
        std::string         _string_term;
        bool                 _is_int_type;
        std::vector<int64_t> _docid_addresses;
        std::vector<int64_t> _doc_freqs;
        Shard::packed_table_ptr _table;
    };


    template <typename term_t>
    class TermDescIterator {
    public:
        typedef TermDesc value_type;
        typedef MergeIterator<term_t> iterator_t;

        TermDescIterator() { }

        TermDescIterator(const iterator_t begin, const iterator_t end) :
                _begin(begin),
                _end(end)
        { }

        const bool hasNext(void) const;

        void next(value_type& data);

    private:
        iterator_t _begin;
        iterator_t _end;
    };


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

    template<typename id_type>
    TermDesc& TermDesc::operator+=(const Term<id_type>& term)
    {
        assert(_int_term == term.int_term());
        assert(_string_term == term.string_term());
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
        term_t& current_id = _begin->first.id();
        while (_begin != _end && _begin->first.id() == current_id) {
            data += *_begin;
            ++_begin;
        }
    }

    template <typename term_t>
    const bool TermDescIterator<term_t>::hasNext(void) const
    {
        return _begin == _end;
    }


} // namespace imhotep

#endif
