#ifndef VAR_INT_VIEW_HPP
#define VAR_INT_VIEW_HPP

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <string>

namespace imhotep {

    class VarIntView
    {
    protected:
        const char* _begin  = 0;
        const char* _end    = 0;

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
    };

    class MMappedVarIntView : public VarIntView
    {
        int    _fd     = 0;
        size_t _length = 0;
        void*  _mapped = 0;

        int _errno = 0;

    public:
        MMappedVarIntView() { }

        MMappedVarIntView(const MMappedVarIntView& rhs) = delete;

        MMappedVarIntView(const std::string& filename)
            : _fd(open(filename.c_str(), O_RDONLY))
            , _errno(errno) {
            if (_fd <= 0) {
                throw std::runtime_error("cannot open file: " + filename +
                                         " " + std::string(strerror(_errno)));
            }

            _length = file_size(_fd);
            _mapped = mmap(0, _length, PROT_READ, MAP_PRIVATE | MAP_POPULATE, _fd, 0);
            _errno  = errno;
            if (_mapped == reinterpret_cast<void*>(-1)) {
                throw std::runtime_error("cannot mmap: " + std::string(strerror(errno)));
            }

            _begin = (const char *) _mapped;
            _end   = _begin + _length;
        }

        ~MMappedVarIntView() {
            if (_mapped) munmap(_mapped, _length);
            if (_fd > 0) close(_fd);
        }

        void * mapped() { return _mapped; }

    private:
        size_t file_size(int fd) {
            struct stat buf;
            int rc(fstat(fd, &buf));
            if (rc != 0) throw std::runtime_error(strerror(errno));
            return buf.st_size;
        }
    };
}

#endif
