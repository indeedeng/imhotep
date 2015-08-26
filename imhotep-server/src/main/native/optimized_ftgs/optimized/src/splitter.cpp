#include "splitter.hpp"

namespace imhotep {

    template <>
    void Splitter<IntTerm>::encode(FILE* file, const IntTerm& term) const {
        write_item(file, term);
    }

    template <>
    void Splitter<StringTerm>::encode(FILE* file, const StringTerm& term) const {
#pragma pack(push, 1)
        class Header {
            const int64_t _doc_offset = 0;
            const int32_t _doc_freq   = 0;
            const size_t  _id_size    = 0;
        public:
            Header(const StringTerm& term)
                : _doc_offset(term.doc_offset())
                , _doc_freq(term.doc_freq())
                , _id_size(term.id().size())
            { }
        };
#pragma pack(pop)
        const Header       header(term);
        const StringRange& id(term.id());
        write_item(file, header);
        write_string(file, id.first, term.id().size());
    }

} // namespace imhotep
