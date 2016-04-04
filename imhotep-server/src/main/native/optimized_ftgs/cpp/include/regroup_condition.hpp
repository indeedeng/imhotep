#ifndef REGROUP_CONDITION
#define REGROUP_CONDITION

#include "docid_iterator.hpp"
#include "shard.hpp"

#include <boost/dynamic_bitset.hpp>
#include <boost/variant.hpp>

#include <cstdint>
#include <ostream>
#include <sstream>
#include <string>
#include <unordered_set>

namespace imhotep {

    class equality_tag   { };
    class inequality_tag { };
    class null_tag       { };

    template <typename term_t, typename comparison_t>
    class Condition {
    public:
        Condition() { }

        Condition(const std::string& field, const term_t& term)
            : _field(field)
            , _term(term)
        { }

        bool operator==(const Condition& rhs) const {
            return field() == rhs.field() && term() == rhs.term();
        }

        const std::string& field() const { return _field; }
        const      term_t&  term() const { return _term;  }

    private:
        std::string _field;
        term_t      _term;
    };

    /**********************************************************************/

    template <typename term_t>
    class EqualityCondition : public Condition<term_t, equality_tag> {
    public:
        EqualityCondition() { }
        EqualityCondition(const std::string& field, const term_t& term)
            : Condition<term_t, equality_tag>(field, term)
        { }

        void reset(Shard& shard);

        bool matches(docid_t doc) {
            const DocIdIterator end;
            while (_doc_it != end && *_doc_it < doc) {
                ++_doc_it;
            }
            return _doc_it != end && *_doc_it == doc;
        }

    private:
        DocIdIterator _doc_it;
    };

    typedef EqualityCondition<int64_t>     IntEquality;
    typedef EqualityCondition<std::string> StrEquality;

    /**********************************************************************/

    template <typename term_t>
    class InequalityCondition : public Condition<term_t, inequality_tag> {
    public:
        InequalityCondition() { }
        InequalityCondition(const std::string& field, const term_t& term)
            : Condition<term_t, inequality_tag>(field, term)
        { }

        void reset(Shard& shard);

        bool matches(docid_t doc) {
            return doc < _doc_ids.size() && _doc_ids.test(doc);
        }

    private:
        boost::dynamic_bitset<> _doc_ids;
    };

    typedef InequalityCondition<int64_t>     IntInequality;
    typedef InequalityCondition<std::string> StrInequality;

    /**********************************************************************/

    template <typename term_t>
    class NullCondition : public Condition<term_t, null_tag> {
    public:
        NullCondition() { }
        NullCondition(const std::string& field, const term_t& term)
            : Condition<term_t, null_tag>(field, term)
        { }

        void reset(Shard& shard) { }

        bool matches(docid_t doc) { return false; }
    };

    typedef NullCondition<int64_t>     IntNull;
    typedef NullCondition<std::string> StrNull;

    /**********************************************************************/

    typedef boost::variant<IntEquality, IntInequality,
                           StrEquality, StrInequality,
                           IntNull, StrNull> RegroupCondition;

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
        std::string operator()(const IntNull& cond)       const { return "IntNull";       }
        std::string operator()(const StrEquality& cond)   const { return "StrEquality";   }
        std::string operator()(const StrInequality& cond) const { return "StrInequality"; }
        std::string operator()(const StrNull& cond)       const { return "StrNull";       }
    };

    struct Reset : public boost::static_visitor<void> {
        Reset(Shard& shard) : _shard(shard) { }

        template <typename Cond>
        void operator()(Cond& cond) const { cond.reset(_shard); }
    private:
        Shard& _shard;
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
