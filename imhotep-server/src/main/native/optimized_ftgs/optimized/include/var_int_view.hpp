#ifndef VAR_INT_VIEW_HPP
#define VAR_INT_VIEW_HPP

#include <cassert>

#include "imhotep_error.hpp"
#include "mmapped_file.hpp"

namespace imhotep {

    class VarIntView
    {
    public:
        VarIntView(const char* begin=0, const char* end=0)
            : _begin(begin) , _end(end)
        { }

        bool operator==(VarIntView const& rhs) const {
            return _begin == rhs._begin && _end == rhs._end;
        }

        const char* begin() const { return _begin; }
        const char* end()   const { return _end;   }

        bool empty() const { return _begin >= _end; }

        uint8_t read() {
            assert(!empty());
            const char result(empty() ? -1 : *_begin);
            ++_begin;
            return result;
        }

        template <typename int_t>
        int_t read_varint(uint8_t b=0) {
            int_t result(0);
            int   shift(0);
            do {
                result |= ((b & 0x7FL) << shift);
                if (b < 0x80) return result;
                shift += 7;
                b = read();
            } while (true);
        }

    private:
        const char* _begin;
        const char* _end;
    };
}

#endif
