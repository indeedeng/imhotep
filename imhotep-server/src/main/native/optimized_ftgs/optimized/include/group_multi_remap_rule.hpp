#ifndef GROUP_MULTI_REMAP_RULE_HPP
#define GROUP_MULTI_REMAP_RULE_HPP

#include <cstdint>
#include <iostream>
#include <utility>
#include <vector>

#include "regroup_condition.hpp"

namespace imhotep {

    class GroupMultiRemapRule {
    public:
        // !@# This could use a better name...
        class Rule {
        public:
            Rule(int32_t                 positive,
                 const RegroupCondition& condition )
                : _positive(positive)
                , _condition(condition)
            { }

            int32_t positive() const { return _positive; }

            const RegroupCondition& condition() const { return _condition; }

            bool operator==(const Rule& rhs) const {
                if (this == &rhs) return true;
                return
                    positive()  == rhs.positive() &&
                    condition() == rhs.condition();
            }

        private:
            int32_t          _positive;
            RegroupCondition _condition;
        };

        typedef std::vector<Rule> Rules;

        GroupMultiRemapRule(int32_t      target,
                            int32_t      negative,
                            const Rules& rules)
            : _target(target)
            , _negative(negative)
            , _rules(rules)
        { }

        int32_t   target() const { return _target;   }
        int32_t negative() const { return _negative; }

        const Rules& rules() const { return _rules; }

        bool operator==(const GroupMultiRemapRule& rhs) const {
            if (this == &rhs) return true;
            return
                target()   == rhs.target()   &&
                negative() == rhs.negative() &&
                rules()    == rhs.rules();
        }

    private:
        int32_t _target;
        int32_t _negative;
        Rules   _rules;
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
       << " target: "   << gmrr.target()
       << " negative: " << gmrr.negative()
       << " rules: { ";
    for (imhotep::GroupMultiRemapRule::Rules::const_iterator it(gmrr.rules().begin());
         it != gmrr.rules().end(); ++it) {
        os << *it << " ";
    }
    os << "} ]";
    return os;
}

#endif
