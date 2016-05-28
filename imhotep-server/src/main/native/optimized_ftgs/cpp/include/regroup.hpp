/** @file regroup.hpp - EXPLODED_MULTISPLIT_REGROUP functor

    You're looking at the Regroup functor used by the JNI nativeRegroup entry
    point to perform an "exploded multiregroup" operation. Here's how it works.

    The client creates a series of GroupMultiRemapRules (GMRRs) organized by
    "target" group. The regroup operation then looks at each document in its
    shard, in order, and if it belongs to a target group, evaluates the
    corresponding list of GMRRs. Each GMRR contains a RegroupCondition; more
    about those later. If we encounter a matching condition as we proceed
    through the list of GMRRs, we reassign the document to the "positive" group
    for that rule and move onto the next document. If we evaluate all conditions
    for the target group without matching, we move the document to a "negative"
    group. Documents in groups that aren't in any defined target group are moved
    to group zero.

    RegroupConditions have an associated field and term and they come in two
    basic flavors: equality and inequality. An equality condition matches a
    document if the document's term for the condition's field matches
    exactly. An inequality condition matches if a doc's term is less than the
    condition's. As there are both flavors of conditions for int and string term
    types, we end up with four kinds of RegroupConditions altogether. There's
    also a special null condition that never matches anything. It's used when
    the client wants to force everything into a particular negative group.

    Bearing all this in mind, there are some implications in light of the fact
    that terms are stored in inverted fashion within a shard. Because we're
    iterating through docs in order and each term in a shard references a list
    of docs stored in order, we're always marching forward through both of them,
    which is a cache-friendly way to proceed given default prefetching
    behavior. To initialize the whole operation, we set up a doc iterator for
    each equality condition conveniently leveraging the shard's per-field btree
    indices.

    That's all well and good for equality conditions, but what about inequality
    conditions? For those we need to know whether a doc matches any terms less
    than the one specified in the condition. The simple way to do that is to
    build a set of docs for each condition containing all docs for all terms up
    until the condition's. How expensive that turns out to be in practice can
    vary wildly depending on term frequency, etc. Fortunately, inequality
    conditions don't seem to appear much in model builds, the context in which
    we hope to leverage this code. In test profiles, this brute-force approach
    doesn't appear to have a visible negative effect, so it's probably premature
    to attempt an optimization of this.
 */

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
