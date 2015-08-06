#ifndef REGROUP_CONDITION
#define REGROUP_CONDITION

#include <cstdint>
#include <string>

namespace imhotep {

    /** !@# For now, a more or less direct translation of the Java class, which
         translation is a bit ugly.
     */
    class RegroupCondition {
    public:
        RegroupCondition(const std::string& field,
                         bool               int_type,
                         int64_t            int_term,
                         const std::string& string_term,
                         bool               inequality)
            : _field(field)
            , _int_type(int_type)
            , _int_term(int_term)
            , _string_term(string_term)
            , _inequality(inequality)
        { }

        const std::string&       field() const { return  _field;      }
        const bool            int_type() const { return _int_type;    }
        const int64_t         int_term() const { return _int_term;    }
        const std::string  string_term() const { return _string_term; }
        const bool          inequality() const { return  _inequality; }

        bool operator==(const RegroupCondition& rhs) const {
            if (this == &rhs) return true;
            return
                inequality() == rhs.inequality() &&
                int_type()   == rhs.int_type()   &&
                field()      == rhs.field()      &&
                (int_type() ?
                 int_term() == rhs.int_term() :
                 string_term() == rhs.string_term());
        }

    private:
        /* !@# TODO:
           - consider whether we can replace strings with StringRanges that
           refer to Java strings (double-byte Java strings could be tricky, but
           maybe with specialized comparator))
           - consider rearranging fields for better packing
         */
        std::string _field;
        bool        _int_type;
        int64_t     _int_term;
        std::string _string_term;
        bool        _inequality;
    };

} // namespace imhotep

#endif
