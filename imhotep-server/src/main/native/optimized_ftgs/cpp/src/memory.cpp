extern "C" {
#include "memory.h"
}

#include "imhotep_error.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <sstream>
#include <string>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>

using std::string;

namespace imhotep {

    class Memory {
    public:
        Memory(const string& dir, const string& prefix, size_t max_size);

        ~Memory();

        const string& filename() const { return _filename; }

        size_t allocated() const { return _end - _begin;         }
        size_t remaining() const { return _length - allocated(); }

        void* malloc(size_t size);
        void* calloc(size_t nmemb, size_t size);

        Memory(const Memory& rhs)            = delete;
        Memory& operator=(const Memory& rhs) = delete;

        static Memory* from(memory_ptr memory) {
            return reinterpret_cast<Memory*>(memory);
        }

        memory_ptr to() { return reinterpret_cast<memory_ptr>(this); }

    private:
        string make_name(const string& dir, const string& prefix);

        int   open_file();
        void* map_file();

        static constexpr size_t _alignment = 64;

        const string _filename;
        const size_t _length;

        const int _fd;
        void*     _mapped;
        char*     _begin;
        char*     _end;

    };

    Memory::Memory(const string& dir, const string& prefix, size_t max_size)
        : _filename(make_name(dir, prefix))
        , _length(max_size)
        , _fd(open_file())
        , _mapped(map_file())
        , _begin(reinterpret_cast<char*>(_mapped))
        , _end(_begin)
    { }

    Memory::~Memory() {
        // !@# bark on failure?
        if (_mapped != (caddr_t) -1) {
            munmap(_mapped, _length);
        }
        if (_fd != -1) {
            close(_fd);
            unlink(_filename.c_str());
        }
    }

    string Memory::make_name(const string& dir, const string& prefix) {
        const std::chrono::steady_clock::time_point tm(std::chrono::steady_clock::now());
        std::ostringstream os;
        os << dir << '/' << prefix << '.'
           << std::chrono::duration_cast<std::chrono::nanoseconds>(tm.time_since_epoch()).count();
        return os.str();
    }

    int Memory::open_file() {
        const int result(open(_filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRWXU));
        if (result == -1) {
            throw imhotep_error(string(__PRETTY_FUNCTION__)
                                + string(": ") + string(strerror(errno)));
        }
        return result;
    }

    void* Memory::map_file() {
        if (lseek(_fd, _length - 1, SEEK_SET) == -1) {
            throw imhotep_error(string(__PRETTY_FUNCTION__)
                                + string(": ") + string(strerror(errno)));
        }

        if (write(_fd, "", 1) != 1) {
            throw imhotep_error(string(__PRETTY_FUNCTION__)
                                + string(": ") + string(strerror(errno)));
        }

        void* result(mmap(0, _length, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0));
        if (result == (caddr_t) -1) {
            throw imhotep_error(string(__PRETTY_FUNCTION__)
                                + string(": ") + string(strerror(errno)));
        }
        return result;
    }

    void* Memory::malloc(size_t size) {
        if (remaining() < size) {
            throw imhotep_error(string(__PRETTY_FUNCTION__) + string(": out of memory"));
        }

        void* result(_end);
        _end += size;

        const size_t lagniappe(std::ptrdiff_t(_end) % _alignment);
        if (lagniappe > 0) {
            _end += (_alignment - lagniappe);
            _end = std::min(_end, _begin + _length);
        }
        return result;
    }

    void* Memory::calloc(size_t nmemb, size_t size) {
        char * current(_end);
        void * result(this->malloc(nmemb * size));
        std::fill(current, _end, 0);
        return result;
    }
}

memory_ptr memory_create(const char* dir, const char* prefix, size_t max_size) {
    imhotep::Memory* result(new imhotep::Memory(dir, prefix, max_size));
    return result->to();
}

void memory_destroy(memory_ptr memory) {
    delete imhotep::Memory::from(memory);
}

void* memory_malloc(memory_ptr memory, size_t size) {
    return imhotep::Memory::from(memory)->malloc(size);
}

void* memory_calloc(memory_ptr memory, size_t nmemb, size_t size) {
    return imhotep::Memory::from(memory)->calloc(nmemb, size);
}

const char* memory_filename(memory_ptr memory) {
    return imhotep::Memory::from(memory)->filename().c_str();
}

size_t memory_allocated(memory_ptr memory) {
    return imhotep::Memory::from(memory)->allocated();
}

size_t memory_remaining(memory_ptr memory) {
    return imhotep::Memory::from(memory)->remaining();
}
