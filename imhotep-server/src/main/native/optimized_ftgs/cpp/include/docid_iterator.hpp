/** @file docid_iterator.hpp

    NOTE: This code relies on varintdecode.c which must be initialized via
    simdvbyteinit() before it will work. That's guaranteed in the context of
    libftgs, however it must be explicitly called within test code, etc.

    TODO(johnf): consider invoking simvbyteinit() in a static block somewhere.
 */
#ifndef DOCID_ITERATOR_HPP
#define DOCID_ITERATOR_HPP

#include "var_int_view.hpp"
#include "varintdecode.h"

#include <boost/iterator/iterator_facade.hpp>

#include <array>

namespace imhotep {

    typedef uint32_t docid_t;   // !@# promote to term.hpp?

    class DocIdIterator
        : public boost::iterator_facade<DocIdIterator,
                                        docid_t const,
                                        boost::forward_traversal_tag> {
    public:
        DocIdIterator() { }

        DocIdIterator(const VarIntView& docid_view,
                      int64_t           doc_offset,
                      int32_t           doc_freq)
            : _remaining(doc_freq)
            , _docid_view(docid_view.begin() + doc_offset, docid_view.end()) {
            refill();
        }

    private:
        friend class boost::iterator_core_access;

        void increment() {
            if (_current_idx < _end_idx) {
                ++_current_idx;
            }
            else if (_current_idx == _end_idx && _remaining > 0) {
                refill();
            }
        }

        /** Shallow check for equality, assumes that either underlying
            docid_views are deeply equal or that other is an end iterator.

            TODO(johnf) consider how to improve this without undue perf penalty.
         */
        bool equal(const DocIdIterator& other) const {
            return
                _remaining     == other._remaining &&
                buffer_count() == other.buffer_count();
        }

        const docid_t& dereference() const { return _buffer[_current_idx]; }

        size_t _current_idx = 0;
        size_t _end_idx     = 0;
        size_t _remaining   = 0;

        std::array<docid_t, 64> _buffer;

        docid_t    _last_value  = 0;
        VarIntView _docid_view;

        size_t buffer_count() const { return _end_idx - _current_idx; }

        void refill() {
            if (_remaining <= 0) return;

            const uint8_t* begin(reinterpret_cast<const uint8_t*>(_docid_view.begin()));
            const size_t   count(std::min(_remaining, _buffer.size()));
            const size_t   bytes_read(masked_vbyte_read_loop_delta(begin, _buffer.data(),
                                                                   count, _last_value));
            _remaining -= count;

            _docid_view  = VarIntView(_docid_view.begin() + bytes_read, _docid_view.end());
            _current_idx = 0;
            _end_idx     = count;
            _last_value  = _buffer.back();
        }
    };
}

#endif
