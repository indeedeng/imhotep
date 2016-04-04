#ifndef GROUP_MULTI_REMAP_RULE_HPP
#define GROUP_MULTI_REMAP_RULE_HPP

#include <algorithm>
#include <cstdint>
#include <ostream>
#include <utility>
#include <vector>

#include "regroup_condition.hpp"

namespace imhotep {

    class GroupMultiRemapRule {
    public:
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
                try {
                    boost::apply_visitor(visitor, _condition);
                }
                catch (const imhotep_error& ex) {
                    /* !@# Some clients, such as ctrmodel, deliberately send
                       conditions with bogus fields in order to create
                       conditions that are never true. In order to support them
                       for now, we replace any rule we can't properly reset with
                       a null condition. */
                    /* !@# whether/how to report this...
                       std::cerr << ex.what() << std::endl;
                    */
                    const std::string field(boost::apply_visitor(FieldOf(), _condition));
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
