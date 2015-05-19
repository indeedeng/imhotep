#ifndef TERM_DESC_ITERATOR_HPP
#define TERM_DESC_ITERATOR_HPP

#include <cassert>
#include <iostream>
#include <vector>

#include <boost/iterator/iterator_facade.hpp>

#include "term_iterator.hpp"

namespace imhotep {

    template <typename term_t>
    struct term_desc {
        typedef typename term_t::id_t id_t;

        id_t                 _id = id_traits<id_t>::default_value();
        std::vector<int64_t> _docid_addresses;
        std::vector<int64_t> _doc_freqs;

        size_t count() const {
            assert(_docid_addresses.size() == _doc_freqs.size());
            return _doc_freqs.size();
        }

        const int64_t* docid_addresses() const { return _docid_addresses.data(); }
        const int64_t*       doc_freqs() const { return _doc_freqs.data();       }

        void reset(const id_t& id) {
            _id = id;
            _docid_addresses.clear();
            _doc_freqs.clear();
        }

        term_desc& operator+=(const term_t& term) {
            assert(_id == term.id());
            _docid_addresses.push_back(term.doc_offset());
            _doc_freqs.push_back(term.doc_freq());
            return *this;
        }
    };


    template <typename iterator_t>
    class term_desc_iterator
        : public boost::iterator_facade<term_desc_iterator<iterator_t>,
                                        term_desc<typename iterator_t::value_type> const,
                                        boost::forward_traversal_tag> {
    public:
        typedef typename iterator_t::value_type term_t;
        typedef term_desc<term_t>               term_desc_t;

        term_desc_iterator() { }

        term_desc_iterator(const iterator_t begin, const iterator_t end)
            : _begin(begin)
            , _end(end) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        void increment() {
            if (_begin == _end) {
                _current = term_desc_t();
                return;
            }

            _current.reset(_begin->id());
            iterator_t it(_begin);
            while (_begin != _end && _begin->id() == _current._id) {
                _current += *_begin;
                ++_begin;
            }
        }

        bool equal(const term_desc_iterator& other) const {
            return
                _begin == other._begin &&
                _end == other._end;
        }

        const term_desc_t& dereference() const { return _current; }

        iterator_t _begin;
        iterator_t _end;

        term_desc_t _current;
    };

} // namespace imhotep


template <typename term_t>
std::ostream& operator<<(std::ostream& os, const imhotep::term_desc<term_t>& desc)
{
    os << desc._id << " {";
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
