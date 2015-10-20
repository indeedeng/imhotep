#include "log.hpp"

#include <syslog.h>

namespace imhotep {

    template<> void Log::log<Log::Emergency>(const std::string& message) { syslog(LOG_EMERG,   "%s", message.c_str()); }
    template<> void Log::log<Log::Alert>(const std::string& message)     { syslog(LOG_ALERT,   "%s", message.c_str()); }
    template<> void Log::log<Log::Critical>(const std::string& message)  { syslog(LOG_CRIT,    "%s", message.c_str()); }
    template<> void Log::log<Log::Error>(const std::string& message)     { syslog(LOG_ERR,     "%s", message.c_str()); }
    template<> void Log::log<Log::Warning>(const std::string& message)   { syslog(LOG_WARNING, "%s", message.c_str()); }
    template<> void Log::log<Log::Notice>(const std::string& message)    { syslog(LOG_NOTICE,  "%s", message.c_str()); }
    template<> void Log::log<Log::Info>(const std::string& message)      { syslog(LOG_INFO,    "%s", message.c_str()); }
    template<> void Log::log<Log::Debug>(const std::string& message)     { syslog(LOG_DEBUG,   "%s", message.c_str()); }

    void Log::emergency(const std::string& message) { log<Emergency>(message); }
    void Log::alert(const std::string& message)     { log<Alert>(message);     }
    void Log::critical(const std::string& message)  { log<Critical>(message);  }
    void Log::error(const std::string& message)     { log<Error>(message);     }
    void Log::warning(const std::string& message)   { log<Warning>(message);   }
    void Log::notice(const std::string& message)    { log<Notice>(message);    }
    void Log::info(const std::string& message)      { log<Info>(message);      }
    void Log::debug(const std::string& message)     { log<Debug>(message);     }

} // namespace imhotep
