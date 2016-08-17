#ifndef LOG_HPP
#define LOG_HPP

#include <string>

namespace imhotep {

    struct Log {
        struct Emergency;
        struct Alert;
        struct Critical;
        struct Error;
        struct Warning;
        struct Notice;
        struct Info;
        struct Debug;

        static void emergency(const std::string& message);
        static void alert(const std::string& message);
        static void critical(const std::string& message);
        static void error(const std::string& message);
        static void warning(const std::string& message);
        static void notice(const std::string& message);
        static void info(const std::string& message);
        static void debug(const std::string& message);

        template <class Level>
        static void log(const std::string& message);
    };

} // namespace imhotep

#endif
