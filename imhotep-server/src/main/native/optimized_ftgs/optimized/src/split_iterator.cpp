#include "split_iterator.hpp"

namespace imhotep {

    template<>
    IntTerm SplitIterator<IntTerm>::decode() {
        return _view.read<IntTerm>();
    }

    template<>
    StringTerm SplitIterator<StringTerm>::decode() {
        const size_t            id_size(_view.read<size_t>());
        const SplitView::Buffer id(_view.read_bytes(id_size));
        const uint64_t          doc_offset(_view.read<uint64_t>());
        const uint64_t          doc_freq(_view.read<uint64_t>());
        return StringTerm(std::string(id.first, id.second), doc_offset, doc_freq);
    }

} // namespace imhotep
