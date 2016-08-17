/** @file group_multi_rmap_rule.hpp - C++ analog to com.indeed.imhotep.GroupMultiRemapRule

    GroupMultiRemapRule represents the same information as its Java peer in a
    marinally less clumsy fashion. (The Java version has to maintain parallel
    arrays of things given that there's no struct available.)

    Conceptually these classes comprise:
      - a map from target group to GroupMultiRemapRule (GMRR)
      - for each GroupMultiRemapRule a negative group and a list of rules
      - for each rule a positive group and a list of RegroupConditions

    See Regroup for the semantics of applying these rules.
    See RegroupCondition for descriptions of the various condition types.

    RegroupConditions maintain state that changes throughout a regroup
    operation, so they're not immutable and thus neither are their containers,
    Rule and GroupMultiRemapRule.
 */
#ifndef GROUP_MULTI_REMAP_RULE_HPP
#define GROUP_MULTI_REMAP_RULE_HPP

#include "regroup_condition.hpp"

#include <algorithm>
#include <cstdint>
#include <ostream>
#include <utility>
#include <vector>

namespace imhotep {

    /** GroupMultiRemapRule associates a negative group with a list of Rules. */
    class GroupMultiRemapRule {
    public:

        /** Rule associates a positive group with a list of RegroupConditions */
        class Rule {
        public:
            Rule() : _positive(0), _condition(IntNull()) { }

            Rule(int32_t                 positive,
                 const RegroupCondition& condition )
                : _positive(positive)
                , _condition(condition)
            { }

            int32_t positive() const { return _positive; }

            const RegroupCondition& condition() const { return _condition; }
                  RegroupCondition& condition()       { return _condition; }

            bool operator==(const Rule& rhs) const {
                if (this == &rhs) return true;
                return
                    positive()  == rhs.positive() &&
                    condition() == rhs.condition();
            }

            void reset(const Reset& visitor) {
                const char* DUMMY_FIELD = "dummyField";
                const std::string field(boost::apply_visitor(FieldOf(), _condition));
                /* !@# Temporary hack to treat 'dummyField' as a special case,
                   since we know that ctrmodel (ab)uses it. In fact, it tends to
                   send large groups of rules with dummy field conditions; we
                   don't want to pay the cost of looking it up in Shard for each
                   and every one. */
                if (field == DUMMY_FIELD) {
                    _condition = IntNull("", 0);
                    return;
                }

                try {
                    boost::apply_visitor(visitor, _condition);
                }
                catch (const imhotep_error& ex) {
                    /* !@# Some clients deliberately send conditions with bogus
                       fields in order to create conditions that are never
                       true. In order to support them for now, we replace any
                       rule we can't properly reset with a null condition. (This
                       is the general case of the 'dummyField' hack above. */
                    _condition = IntNull(field, 0);
                }
            }

            void reset(Shard& shard) { reset(Reset(shard)); }

        private:
            int32_t          _positive;
            RegroupCondition _condition;
        };

        typedef std::vector<Rule> Rules;

        GroupMultiRemapRule() : _negative(0), _rules(0) { }

        GroupMultiRemapRule(int32_t      negative,
                            const Rules& rules)
            : _negative(negative)
            , _rules(rules)
        { }

        int32_t negative() const { return _negative; }

        const Rules& rules() const { return _rules; }
              Rules& rules()       { return _rules; }

        bool operator==(const GroupMultiRemapRule& rhs) const {
            if (this == &rhs) return true;
            return
                negative() == rhs.negative() &&
                rules()    == rhs.rules();
        }

        void reset(Shard& shard) {
            const Reset visitor(shard);
            for (Rules::iterator it(_rules.begin()); it != _rules.end(); ++it) {
                it->reset(visitor);
            }
        }

    private:
        int32_t _negative;
        Rules   _rules;
    };

    /** GroupMultiRemapRules maintains a map of target group to GMRR using the
        good ol' sorted vector of pairs scheme. The tradeoff here is O(log n)
        lookup time vs. an extra pointer hop (stl::unordered_map stores pointers
        to chains internally). Generally this collection is small, so the
        vector-based solution should win. In my ad hoc tests it appeared to beat
        std::unordered_map. It's certainly easy enough to toggle between
        implementations should looking up GMRRs ever show up as a hot spot.
     */
    typedef std::pair<int32_t, GroupMultiRemapRule> GMRREntry; // !@# fix int32_t -- use group_t
    class GroupMultiRemapRules : public std::vector<GMRREntry> {
    public:
        void sort() {
            std::sort(begin(), end(),
                      [](const GMRREntry& thing1, const GMRREntry& thing2) {
                          return thing1.first < thing2.first;
                      });
        }

        iterator find(int32_t group) {
            const GMRREntry target(group, GroupMultiRemapRule());
            iterator it(std::lower_bound(begin(), end(), target,
                                         [](const GMRREntry& thing1, const GMRREntry& thing2) {
                                             return thing1.first < thing2.first;
                                         }));
            return it != end() && (*it).first == group ? it : end();
        }
    };

} // namespace imhotep

inline
std::ostream& operator<<(std::ostream& os, const imhotep::GroupMultiRemapRule::Rule& rule) {
    os << "[Rule"
       << " positive: "  << rule.positive()
       << " condition: " << rule.condition()
       << "]";
    return os;
}

inline
std::ostream& operator<<(std::ostream& os, const imhotep::GroupMultiRemapRule& gmrr) {
    os << "[GroupMultiRemapRule"
       << " negative: " << gmrr.negative()
       << " rules: { ";
    for (imhotep::GroupMultiRemapRule::Rules::const_iterator it(gmrr.rules().begin());
         it != gmrr.rules().end(); ++it) {
        os << *it << " ";
    }
    os << "} ]";
    return os;
}

inline
std::ostream& operator<<(std::ostream& os, const imhotep::GroupMultiRemapRules& gmrrs) {
    os << "[GroupMultiRemapRules ";
    for (auto entry: gmrrs) {
        os << "{target: " << entry.first
           << " gmrr: "   << entry.second
           << "}";
    }
    os << "]";
    return os;
}

#endif
