#ifndef TIMER_HPP
#define TIMER_HPP

#include <sys/times.h>

namespace imhotep {

    /** A relatively lightweight utility based on times() for measuring elapsed
        time of a scope.

        Example:

          clock_t elapsed;
          {
             Timer timer(&elapsed);
          }
          std::cerr << "elapsed: " << Timer::secs(elapsed) << std::endl;

        TODO(johnf): consider passing a 'report' function into the c-tor that
        gets automatically invoked during the dtor.
     */
    class Timer {
        struct tms    _buf;
        const clock_t _before;
        clock_t&      _elapsed;

    public:
        Timer(clock_t& elapsed)
            : _before(times(&_buf))
            , _elapsed(elapsed)
        { }

        ~Timer() {
            const clock_t _after(times(&_buf));
            _elapsed = _after - _before; // !@# overflow is possible
        }

        static double secs(clock_t ticks,
                           long    ticks_per_sec = sysconf(_SC_CLK_TCK)) {
            return ticks * 1. / ticks_per_sec;
        }
    };

} // namespace imhotep

#endif
