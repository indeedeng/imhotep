#ifndef TERM_HPP
#define TERM_HPP

#include <stdint.h>
#include <iostream>

namespace imhotep {

    template <typename TYPE>
    class Term {
        TYPE    _id         = 0;
        int64_t _doc_offset = 0;
        int64_t _doc_freq   = 0;

    public:
        typedef TYPE id_type;

        Term() = default;

        Term(const TYPE& id, int64_t doc_offset, int64_t doc_freq)
            : _id(id)
            , _doc_offset(doc_offset)
            , _doc_freq(doc_freq)
        { }

        const TYPE& id()         const { return _id;         }
        int64_t     doc_offset() const { return _doc_offset; }
        int64_t     doc_freq()   const { return _doc_freq;   }

        bool operator<(const TermType& rhs) const { return id() < rhs.id(); }

        bool is_null() const { return *this == Term(); }
    };

    typedef Term<int64_t> IntTerm;

    typedef uint64_t StringTermId;
    typedef Term<StringTermId> StringTerm;

} // namespace imhotep

template<typename TYPE>
std::ostream&
operator<<(std::ostream& os, const imhotep::Term<TYPE>& term)
{
    os << "id: "          << term.id()
       << " doc_offset: " << term.doc_offset()
       << " doc_freq: "   << term.doc_freq();
    return os;
}

#endif
