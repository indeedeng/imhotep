#ifndef REGROUP_HPP
#define REGROUP_HPP

#include "doc_to_group.hpp"
#include "group_multi_remap_rule.hpp"
#include "shard.hpp"

#include <vector>

namespace imhotep {

    struct Eval : public boost::static_visitor<bool> {

        Eval(DocToGroup::doc_t doc) : _doc(doc) { }

        template <typename Cond>
        bool operator()(Cond& cond) const { return cond.matches(_doc); }

    private:
        DocToGroup::doc_t _doc;
    };

    class Regroup {
    public:
        Regroup(GroupMultiRemapRules& gmrrs,
                Shard&                shard,
                DocToGroup&           dtgs);

        void operator()();

    private:
        void reset_conditions();

        void eval(GroupMultiRemapRule& gmrr, DocToGroup::doc_t doc);

        bool eval(GroupMultiRemapRule::Rule& rule, DocToGroup::doc_t doc);

        GroupMultiRemapRules& _gmrrs;
        Shard&                _shard;
        DocToGroup&           _dtgs;
    };

    inline
    void Regroup::eval(GroupMultiRemapRule& gmrr, DocToGroup::doc_t doc) {
        for (GroupMultiRemapRule::Rules::iterator it(gmrr.rules().begin());
             it != gmrr.rules().end(); ++it) {
            if (eval(*it, doc)) {
                _dtgs.set(doc, it->positive());
                return;
            }
        }
        _dtgs.set(doc, gmrr.negative());
    }

    inline
    bool Regroup::eval(GroupMultiRemapRule::Rule& rule, DocToGroup::doc_t doc) {
        RegroupCondition& condition(rule.condition());
        const Eval eval_visitor(doc);
        const bool result(boost::apply_visitor(eval_visitor, condition));
        return result;
    }


} // namespace imhotep

#endif
