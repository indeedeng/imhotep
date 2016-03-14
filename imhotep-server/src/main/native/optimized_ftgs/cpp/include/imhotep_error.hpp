#ifndef IMHOTEP_ERROR
#define IMHOTEP_ERROR

#include <stdexcept>
#include <string>

namespace imhotep {

    class imhotep_error : public std::runtime_error {
    public:
        imhotep_error(const std::string& what)
            : std::runtime_error(what)
        { }
    };
}

#endif
