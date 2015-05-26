#ifndef TERM_DESC_ITERATOR_HPP
#define TERM_DESC_ITERATOR_HPP

#include <cassert>
#include <iostream>
#include <vector>

#include <boost/iterator/iterator_facade.hpp>

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
    class TermDescIterator
        : public boost::iterator_facade<TermDescIterator<term_t>,
                                        TermDesc const,
                                        boost::forward_traversal_tag> {
    public:
        typedef MergeIterator<term_t> iterator_t;

        TermDescIterator() { }

        TermDescIterator(const iterator_t begin, const iterator_t end) :
                _begin(begin),
                _end(end)
        {
            increment();
        }


    private:
        friend class boost::iterator_core_access;

        void increment();

        bool equal(const TermDescIterator& other) const;

        const TermDesc& dereference() const { return _current; }

        iterator_t _begin;
        iterator_t _end;

        TermDesc _current;
    };


    size_t TermDesc::count() const
    {
        assert(_docid_addresses.size() == _doc_freqs.size());
        return _doc_freqs.size();
    }

    void TermDesc::reset(const int64_t id, Shard::packed_table_ptr table)
    {
        _int_term = id;
        _is_int_type = true;
        _string_term.clear();
        _docid_addresses.clear();
        _doc_freqs.clear();
        _table = table;
    }

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
    void TermDescIterator<term_t>::increment()
    {
        if (_begin == _end) {
            _current = TermDesc();
            return;
        }

        _current.reset(_begin->first.id(), _begin->second);
        term_t& current_id = _begin->first.id();
        iterator_t it(_begin);
        while (_begin != _end && _begin->first.id() == current_id) {
            _current += *_begin;
            ++_begin;
        }
    }

    template <typename term_t>
    bool TermDescIterator<term_t>::equal(const TermDescIterator& other) const
    {
        return _begin == other._begin && _end == other._end;
    }


} // namespace imhotep

#endif
