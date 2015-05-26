#ifndef TERM_DESC_ITERATOR_HPP
#define TERM_DESC_ITERATOR_HPP

#include <cassert>
#include <iostream>
#include <vector>

#include <boost/iterator/iterator_facade.hpp>

#include "shard.hpp"

namespace imhotep {

    template <typename term_t>
    class TermDesc {
    public:
        typedef typename term_t::id_t id_t;

        const id_t& id() const { return _id; }

        const int64_t* docid_addresses() const { return _docid_addresses.data(); }
        const int64_t*       doc_freqs() const { return _doc_freqs.data();       }

        Shard::packed_table_ptr table() { return _table; }

        size_t count() const;

        void reset(const id_t& id, Shard::packed_table_ptr table);

        TermDesc& operator+=(const term_t& term);

    private:
        id_t                    _id = IdTraits<id_t>::default_value();
        std::vector<int64_t>    _docid_addresses;
        std::vector<int64_t>    _doc_freqs;
        Shard::packed_table_ptr _table;
    };


    template <typename term_t>
    class TermDescIterator
        : public boost::iterator_facade<TermDescIterator<term_t>,
                                        TermDesc<term_t> const,
                                        boost::forward_traversal_tag> {
    public:
        typedef TermDesc<term_t>      term_desc_t;
        typedef MergeIterator<term_t> iterator_t;

        TermDescIterator() { }

        TermDescIterator(const iterator_t begin,
                         const iterator_t end);

    private:
        friend class boost::iterator_core_access;

        void increment();

        bool equal(const TermDescIterator& other) const;

        const term_desc_t& dereference() const { return _current; }

        iterator_t _begin;
        iterator_t _end;

        term_desc_t _current;
    };


    template <typename term_t>
    size_t TermDesc<term_t>::count() const
    {
        assert(_docid_addresses.size() == _doc_freqs.size());
        return _doc_freqs.size();
    }

    template <typename term_t>
    void TermDesc<term_t>::reset(const id_t& id, Shard::packed_table_ptr table)
    {
        _id = id;
        _docid_addresses.clear();
        _doc_freqs.clear();
        _table = table;
    }

    template <typename term_t>
    TermDesc<term_t>& TermDesc<term_t>::operator+=(const term_t& term)
    {
        assert(_id == term.id());
        _docid_addresses.push_back(term.doc_offset());
        _doc_freqs.push_back(term.doc_freq());
        return *this;
    }


    template <typename iterator_t>
    TermDescIterator<iterator_t>::TermDescIterator(const iterator_t begin,
                                                   const iterator_t end)
        : _begin(begin)
        , _end(end)
    {
        increment();
    }

    template <typename iterator_t>
    void TermDescIterator<iterator_t>::increment()
    {
        if (_begin == _end) {
            _current = term_desc_t();
            return;
        }

        _current.reset(_begin->first.id(), _begin->second);
        iterator_t it(_begin);
        while (_begin != _end && _begin->first.id() == _current.id()) {
            _current += (*_begin).first;
            ++_begin;
        }
    }

    template <typename iterator_t>
    bool TermDescIterator<iterator_t>::equal(const TermDescIterator& other) const
    {
        return _begin == other._begin && _end == other._end;
    }


} // namespace imhotep


template <typename term_t>
std::ostream& operator<<(std::ostream& os, const imhotep::TermDesc<term_t>& desc)
{
    os << desc.id() << " {";
    for (size_t index(0); index < desc.count(); ++index) {
        if (index != 0) os << ' ';
        os << "( " << desc.docid_addresses()[index]
           << ", " << desc.doc_freqs()[index]
           << " )";
    }
    os << '}';
    return os;
}

#endif
