#include "split_iterator.hpp"

namespace imhotep {

    template<>
    IntTerm SplitIterator<IntTerm>::decode(std::istream& is) {
        IntTerm result;
        is.read(reinterpret_cast<char*>(&result), sizeof(IntTerm));
        return result;
    }

    template<>
    StringTerm SplitIterator<StringTerm>::decode(std::istream& is) {
        size_t   id_size(0);
        uint64_t doc_offset(0);
        uint64_t doc_freq(0);
        is.read(reinterpret_cast<char*>(&id_size), sizeof(id_size));
        _id_buffer.resize(id_size);
        is.read(_id_buffer.data(), id_size);
        is.read(reinterpret_cast<char *>(&doc_offset), sizeof(doc_offset));
        is.read(reinterpret_cast<char *>(&doc_freq), sizeof(doc_freq));
        return StringTerm(std::string(_id_buffer.begin(), _id_buffer.end()),
                          doc_offset, doc_freq);
    }

} // namespace imhotep
