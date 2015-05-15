#ifndef TERM_HPP
#define TERM_HPP

#include <stdint.h>
#include <iostream>
#include <string>

namespace imhotep {

    template <typename term_t>
    class Term {
        term_t  _id         = 0;
        int64_t _doc_offset = 0;
        int64_t _doc_freq   = 0;

    public:
        typedef term_t id_type;

        Term() = default;

        Term(const term_t& id, int64_t doc_offset, int64_t doc_freq)
            : _id(id)
            , _doc_offset(doc_offset)
            , _doc_freq(doc_freq)
        { }

        const term_t& id() const { return _id; }

        int64_t doc_offset() const { return _doc_offset; }
        int64_t   doc_freq() const { return _doc_freq;   }

        bool operator==(const Term& rhs) const { return _id == rhs._id; }
        bool operator<(const Term& rhs) const { return id() < rhs.id(); }
    };

    typedef Term<int64_t>     IntTerm;
    typedef Term<std::string> StringTerm;

} // namespace imhotep

template<typename term_t>
std::ostream&
operator<<(std::ostream& os, const imhotep::Term<term_t>& term)
{
    os << "id: "          << term.id()
       << " doc_offset: " << term.doc_offset()
       << " doc_freq: "   << term.doc_freq();
    return os;
}

#endif
