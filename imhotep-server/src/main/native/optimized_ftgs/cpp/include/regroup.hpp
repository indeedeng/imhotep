#ifndef REGROUP_HPP
#define REGROUP_HPP

namespace imhotep {

    class Regroup {
        const std::vector<GroupMultiRemapRule>& _gmrrs;

    public:
        Regroup(const std::vector<GroupMultiRemapRule>& gmrrs)
            : _gmrrs(gmrrs)
        { }

        
    };

}

#endif
