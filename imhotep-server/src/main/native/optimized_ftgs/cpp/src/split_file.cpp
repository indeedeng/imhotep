#include "split_file.hpp"

#include "imhotep_error.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <sstream>

namespace imhotep {

    SplitFile::SplitFile(const std::string& filename)
        : _filename(filename)
        , _fd(open(filename.c_str(), O_CREAT | O_RDWR, S_IRWXU))
    {
        if (_fd == -1) {
            throw imhotep_error(std::string(__FUNCTION__) +
                                " failed to open file: " + filename +
                                " " + std::string(strerror(errno)));
        }

        /* Unix unlink()/remove() semantics dictate that a file is not really
           removed until it is closed. We unlink immediately so the file will be
           cleaned in a crash scenario. (IMTEPD-199) */
        if (remove(filename.c_str()) != 0) {
            throw imhotep_error(std::string(__FUNCTION__) +
                                " failed to remove temp file: " + filename +
                                " " + std::string(strerror(errno)));
        }
    }

    SplitFile::~SplitFile()
    {
        if (mapped()) {
            if (munmap(_addr, _length) != 0) {
            std::cerr << __FUNCTION__ << " failed to unmap temp file "
                      << to_string() << std::endl;
            }
        }

        if (close(_fd) != 0) {
            std::cerr << __FUNCTION__ << " failed to close temp file "
                      << to_string() << std::endl;
        }
    }

    SplitView SplitFile::split_view()
    {
        map();
        const char* begin(reinterpret_cast<const char*>(_addr));
        const char* end(begin + _length);
        return SplitView(begin, end);
    }

    std::string SplitFile::to_string() const
    {
        std::ostringstream os;
        os << "SplitFile "
           << " _filename: " << _filename
           << " _fd: "       << _fd
           << " _addr: "     << _addr
           << " _length: "   << _length;
        return os.str();
    }

    size_t SplitFile::file_size() const
    {
        const off_t result(lseek(_fd, 0, SEEK_END));
        if (result == -1) {
            throw imhotep_error(std::string(__FUNCTION__) +
                                " failed to lseek " + to_string() +
                                " " + std::string(strerror(errno)));
        }
        return result;
    }

    void SplitFile::map()
    {
        std::lock_guard<std::mutex> lock_map(_map_mutex);

        if (mapped()) return;

        if (_addr != 0 || _length != 0) {
            throw imhotep_error(std::string(__FUNCTION__) +
                                " invalid mapped state " + to_string());
        }

        _length = file_size();
        if (_length > 0) {
            _addr   = mmap(0, _length, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd(), 0);
            if (_addr == reinterpret_cast<void*>(-1)) {
                throw imhotep_error(std::string(__FUNCTION__) +
                                    " failed to mmap " + to_string() +
                                    " " + std::string(strerror(errno)));
            }
        }
    }
}
