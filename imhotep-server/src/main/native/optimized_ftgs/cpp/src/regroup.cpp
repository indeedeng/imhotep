#include "regroup.hpp"

#include "docid_iterator.hpp"

#include <unordered_map>

namespace imhotep {

    Regroup::Regroup(GroupMultiRemapRules& gmrrs,
                     Shard&                shard,
                     DocToGroup&           dtgs)
        : _gmrrs(gmrrs)
        , _shard(shard)
        , _dtgs(dtgs)
    { }

    void Regroup::operator()()
    {
        reset_conditions();

        for (DocToGroup::doc_t doc(_dtgs.begin()); doc != _dtgs.end(); ++doc) {
            const DocToGroup::group_t      current(_dtgs.get(doc));
            GroupMultiRemapRules::iterator it(_gmrrs.find(current));
            if (it != _gmrrs.end()) {
                eval(it->second, doc);
            }
            else {
                _dtgs.set(doc, 0);
            }
        }
    }

    void Regroup::reset_conditions()
    {
        for (GroupMultiRemapRules::iterator it(_gmrrs.begin()); it != _gmrrs.end(); ++it) {
            GroupMultiRemapRule& gmrr(it->second);
            gmrr.reset(_shard);
        }
    }

} // namespace imhotep
