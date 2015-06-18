#ifndef TERM_HPP
#define TERM_HPP

#include <stdint.h>

#include <functional>
#include <iostream>
#include <sstream>
#include <string>

#include "log.hpp"

namespace imhotep {

    enum Encoding:int { STRING_TERM_TYPE=0, INT_TERM_TYPE=1 };

    template <typename id_type>
    struct IdTraits {
        static id_type default_value();
        static Encoding encode();
    };

    template <typename term_t>
    struct TermTraits {
        static std::string  term_file_extension();
        static std::string docid_file_extension();
    };

    template <typename id_type>
    class Term {
        id_type _id         = IdTraits<id_type>::default_value();
        int64_t _doc_offset = 0;
        int32_t _doc_freq   = 0;

    public:
        typedef id_type id_t;

        Term() = default;

        Term(const id_type& id, int64_t doc_offset, int32_t doc_freq)
            : _id(id)
            , _doc_offset(doc_offset)
            , _doc_freq(doc_freq)
        { }

        const id_type& id() const { return _id; }

        int64_t doc_offset() const { return _doc_offset; }
        int32_t   doc_freq() const { return _doc_freq;   }

        bool operator==(const Term& rhs) const {
            return
                id()         == rhs.id()         &&
                doc_offset() == rhs.doc_offset() &&
                doc_freq()   == rhs.doc_freq();
        }

        bool operator<(const Term& rhs) const {
            std::ostringstream os;
            return id() < rhs.id() || (id() == rhs.id() && doc_offset() < rhs.doc_offset());
        }

        bool operator>(const Term& rhs) const {
            return id() > rhs.id() || (id() == rhs.id() && doc_offset() > rhs.doc_offset());
        }

        bool empty() const { return _id == IdTraits<id_type>::default_value(); }

        size_t hash() const { return 0; }
    };

    typedef Term<int64_t>     IntTerm;
    typedef Term<std::string> StringTerm;

    template <>
    struct IdTraits<int64_t> {
        static int64_t default_value() { return 0; }
        static Encoding encode() { return INT_TERM_TYPE;  }
    };

    template <>
    struct IdTraits<std::string> {
        static std::string default_value() { return std::string(); }
        static Encoding encode() { return STRING_TERM_TYPE;  }
    };

    template <>
    struct TermTraits<IntTerm> {
        static std::string  term_file_extension() { return "intterms"; }
        static std::string docid_file_extension() { return "intdocs";  }
    };

    template <>
    struct TermTraits<StringTerm> {
        static std::string  term_file_extension() { return "strterms"; }
        static std::string docid_file_extension() { return "strdocs";  }
    };

    template <> inline
    size_t Term<int64_t>::hash() const {
        static std::hash<int64_t> fun;
        return fun(id());
    }

    template <> inline
    size_t Term<std::string>::hash() const {
        static std::hash<std::string> fun;
        return fun(id());
    }

} // namespace imhotep


template<typename id_type>
std::ostream&
operator<<(std::ostream& os, const imhotep::Term<id_type>& term)
{
    os << "id: "          << term.id()
       << " doc_offset: " << term.doc_offset()
       << " doc_freq: "   << term.doc_freq();
    return os;
}

#endif
