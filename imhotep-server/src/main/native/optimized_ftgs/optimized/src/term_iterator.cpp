#include "term_iterator.hpp"

namespace imhotep {

    template<>
    void TermIterator<IntTerm>::increment() {
        if (!_term_view.empty()) {
            const int64_t id_delta(_term_view.read_varint<int64_t>(_term_view.read()));
            const int64_t offset_delta(_term_view.read_varint<int64_t>(_term_view.read()));
            const int64_t doc_freq(_term_view.read_varint<int32_t>(_term_view.read()));
            _current = IntTerm(_current.id() + id_delta,
                               _current.doc_offset() + offset_delta,
                               doc_freq);
        }
        else {
            _current = IntTerm();
        }
    }

    template<>
    void TermIterator<StringTerm>::increment() {
        if (!_term_view.empty()) {
            const int64_t erase(_term_view.read_varint<int64_t>(_term_view.read()));
            const int64_t append(_term_view.read_varint<int64_t>(_term_view.read()));
            _id_buffer.erase(_id_buffer.size() - erase, erase);
            for (size_t count(0); count < size_t(append); ++count) {
                _id_buffer.push_back(_term_view.read());
            }

            const int64_t offset_delta(_term_view.read_varint<int64_t>(_term_view.read()));
            const int64_t doc_freq(_term_view.read_varint<int64_t>(_term_view.read()));
            _current = StringTerm(StringRange(_id_buffer),
                                  _current.doc_offset() + offset_delta,
                                  doc_freq);
        }
        else {
            _current = StringTerm();
        }
    }

} // namespace imhotep
