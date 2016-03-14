#ifndef REGROUP_CONDITION
#define REGROUP_CONDITION

#include <boost/variant.hpp>

#include <cstdint>
#include <ostream>
#include <sstream>
#include <string>

namespace imhotep {

    class EqualityCondition   { };
    class InequalityCondition { };

    template <typename TermType, typename EqualityType>
    class Condition {
    public:
        Condition() { }

        Condition(const std::string& field, const TermType& term)
            : _field(field)
            , _term(term)
        { }

        bool operator==(const Condition& rhs) const {
            return field() == rhs.field() && term() == rhs.term();
        }

        const std::string& field() const { return _field; }
        const    TermType&  term() const { return _term;  }

    private:
        const std::string _field;
        const TermType    _term;
    };

    typedef Condition<int64_t, EqualityCondition>   IntEquality;
    typedef Condition<int64_t, InequalityCondition> IntInequality;

    typedef Condition<std::string, EqualityCondition>   StrEquality;
    typedef Condition<std::string, InequalityCondition> StrInequality;

    typedef boost::variant<IntEquality, IntInequality,
                           StrEquality, StrInequality> RegroupCondition;

    struct FieldOf : public boost::static_visitor<std::string> {
        template <typename Cond>
        std::string operator()(const Cond& cond) const { return cond.field(); }
    };

    struct TermOf : public boost::static_visitor<std::string> {
        template <typename Cond>
        const std::string operator()(const Cond& cond) const {
            std::stringstream ss;
            ss << cond.term();
            return ss.str();
        }
    };

    struct KindOf : public boost::static_visitor<std::string> {
        std::string operator()(const IntEquality& cond)   const { return "IntEquality";   }
        std::string operator()(const IntInequality& cond) const { return "IntInequality"; }
        std::string operator()(const StrEquality& cond)   const { return "StrEquality";   }
        std::string operator()(const StrInequality& cond) const { return "StrInequality"; }
    };

} // namespace imhotep

inline
std::ostream& operator<<(std::ostream& os, const imhotep::RegroupCondition& condition) {
    os << "[RegroupCondition "
       << " kind: "  << boost::apply_visitor(imhotep::KindOf(), condition)
       << " field: " << boost::apply_visitor(imhotep::FieldOf(), condition)
       << " term: "  << boost::apply_visitor(imhotep::TermOf(), condition)
       << "]";
    return os;
}

#endif
