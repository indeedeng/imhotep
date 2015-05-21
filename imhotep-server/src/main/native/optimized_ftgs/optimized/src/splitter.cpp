#include "splitter.hpp"

namespace imhotep {

    template <>
    void Splitter<IntTerm>::encode(std::ostream& os, const IntTerm& term) {
        os.write(reinterpret_cast<const char *>(&term), sizeof(IntTerm));
    }

    template <>
    void Splitter<StringTerm>::encode(std::ostream& os, const StringTerm& term) {
        const std::string& id(term.id());
        const size_t       id_size(id.size());
        const uint64_t     doc_offset(term.doc_offset());
        const uint64_t     doc_freq(term.doc_freq());
        os.write(reinterpret_cast<const char*>(&id_size), sizeof(id_size));
        os.write(id.data(), id.size());
        os.write(reinterpret_cast<const char*>(&doc_offset), sizeof(doc_offset));
        os.write(reinterpret_cast<const char*>(&doc_freq), sizeof(doc_freq));
    }

} // namespace imhotep
