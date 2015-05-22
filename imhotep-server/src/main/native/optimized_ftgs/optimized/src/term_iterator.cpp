#include "term_iterator.hpp"

namespace imhotep {

    template<>
    void TermIterator<IntTerm>::increment() {
        if (!_term_view->empty()) {
            VarIntView& view(*_term_view);
            const int64_t id_delta(view.read_varint<int64_t>(view.read()));
            const int64_t offset_delta(view.read_varint<int64_t>(view.read()));
            const int64_t doc_freq(view.read_varint<int64_t>(view.read()));
            _current = IntTerm(_current.id() + id_delta,
                               _docid_base + _current.doc_offset() + offset_delta,
                               doc_freq);
        }
        else {
            _term_view = std::make_shared<MMappedVarIntView>();
        }
    }

    template<>
    void TermIterator<StringTerm>::increment() {
        if (!_term_view->empty()) {
            VarIntView& view(*_term_view);

            const int64_t erase(view.read_varint<int64_t>(view.read()));
            const int64_t append(view.read_varint<int64_t>(view.read()));
            _id_buffer.erase(_id_buffer.size() - erase, erase);
            for (size_t count(0); count < size_t(append); ++count) {
                _id_buffer.push_back(view.read());
            }

            const int64_t offset_delta(view.read_varint<int64_t>(view.read()));
            const int64_t doc_freq(view.read_varint<int64_t>(view.read()));
            _current = StringTerm(_id_buffer,
                                  _docid_base + _current.doc_offset() + offset_delta,
                                  doc_freq);
        }
        else {
            _term_view = std::make_shared<MMappedVarIntView>();
        }
    }

} // namespace imhotep
