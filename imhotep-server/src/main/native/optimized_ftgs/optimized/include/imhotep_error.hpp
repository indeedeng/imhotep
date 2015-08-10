#ifndef IMHOTEP_ERROR
#define IMHOTEP_ERROR

#include <execinfo.h>

#include <sstream>
#include <stdexcept>
#include <string>

namespace imhotep {

    class imhotep_error : public std::runtime_error {
    public:
        imhotep_error(const std::string& what)
            : std::runtime_error(decorate(what))
        { }

    private:
        static std::string decorate(const std::string& what) {
            std::stringstream message;
            message << what << std::endl;

            void*     trace[1024];
            const int num_addresses(backtrace(trace, 1024));
            char**    symbols(backtrace_symbols(trace, num_addresses));
            if (symbols) {
                for (int index(0); index < num_addresses; ++index) {
                    message << symbols[index] << std::endl;
                }
            }
            else {
                message << "(no stack trace available)";
            }
            return message.str();
            }
    };
}

#endif
