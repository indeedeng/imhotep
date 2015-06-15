#include "splitter.hpp"

namespace imhotep {

    template <>
    void Splitter<IntTerm>::encode(std::ostream& os, const IntTerm& term) {
        // os.write(reinterpret_cast<const char *>(&term), sizeof(IntTerm));
        const int64_t  id(term.id());
        const int64_t doc_offset(term.doc_offset());
        const int32_t doc_freq(term.doc_freq());
        os.write(reinterpret_cast<const char*>(&id), sizeof(id));
        os.write(reinterpret_cast<const char*>(&doc_offset), sizeof(doc_offset));
        os.write(reinterpret_cast<const char*>(&doc_freq), sizeof(doc_freq));
    }

    template <>
    void Splitter<StringTerm>::encode(std::ostream& os, const StringTerm& term) {
        const std::string& id(term.id());
        const size_t       id_size(id.size());
        const int64_t     doc_offset(term.doc_offset());
        const int32_t     doc_freq(term.doc_freq());
        os.write(reinterpret_cast<const char*>(&id_size), sizeof(id_size));
        os.write(id.c_str(), id.size());
        os.write(reinterpret_cast<const char*>(&doc_offset), sizeof(doc_offset));
        os.write(reinterpret_cast<const char*>(&doc_freq), sizeof(doc_freq));
    }

} // namespace imhotep
