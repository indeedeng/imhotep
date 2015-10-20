#ifndef GROUP_REMAP_RULE_HPP
#define GROUP_REMAP_RULE_HPP

#include <cstdint>

#include "regroup_condition.hpp"

namespace imhotep {

    class GroupRemapRule {
    public:
        GroupRemapRule(int32_t                 target,
                       int32_t                 positive,
                       int32_t                 negative,
                       const RegroupCondition& condition)
            : _target(target)
            , _positive(positive)
            , _negative(negative)
            , _condition(condition)
        { }

        int32_t   target() const { return _target;   }
        int32_t positive() const { return _positive; }
        int32_t negative() const { return _negative; }

        const RegroupCondition& condition() const { return _condition; }

        bool operator==(const GroupRemapRule& rhs) const {
            if (this == &rhs) return true;
            return
                target()    == rhs.target()   &&
                positive()  == rhs.positive() &&
                negative()  == rhs.negative() &&
                condition() == rhs.condition();
        }

    private:
        const int32_t          _target;
        const int32_t          _positive;
        const int32_t          _negative;
        const RegroupCondition _condition;
    };

} // namespace imhotep


#endif
