#ifndef MMAPPED_FILE_HPP
#define MMAPPED_FILE_HPP

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>

#include "imhotep_error.hpp"

namespace imhotep {

    class OpenedFile {
    public:
        /** Normal c-tor opens the file and optionally deletes it on close. */
        OpenedFile(const std::string& filename, bool delete_on_close=false)
            : _filename(filename)
            , _delete_on_close(delete_on_close)
            , _fd(open(filename.c_str(), O_RDONLY)) {

            if (_fd == -1) {
                throw imhotep_error("cannot open file: " + filename +
                                    " " + std::string(strerror(errno)));
            }
            else if (_delete_on_close) {
                /* Unix unlink()/remove() semantics are that a file is not
                   really removed until it is closed. We unlink immediately so
                   the file will be cleaned in a crash scenario. (IMTEPD-199) */
                remove(filename.c_str()); // TODO(johnf): Figure out how to warn here...
            }

            struct stat buf;
            const int rc(fstat(_fd, &buf));
            if (rc != 0) {
                close(_fd);
                if (_delete_on_close) {
                    remove(_filename.c_str());
                }
                throw imhotep_error(strerror(errno));
            }
            _size = buf.st_size;
        }

        /** For files already opened in Javaland, we just record the size. */
        OpenedFile(const std::string& filename, int fd=-1)
            : _filename(filename)
            , _delete_on_close(false)
            , _fd(fd)
            , _size(0)  {
            struct stat info;
            if (stat(filename.c_str(), &info) != 0) {
                throw imhotep_error("cannot stat file: " + filename +
                                    " " + std::string(strerror(errno)));
            }
            _size = info.st_size;
        }

        ~OpenedFile() {
            if (_fd != -1) {
                close(_fd);
                if (_delete_on_close) {
                    remove(_filename.c_str());
                }
            }
        }

        OpenedFile(const OpenedFile& rhs)            = delete;
        OpenedFile& operator=(const OpenedFile& rhs) = delete;

        int fd() const { return _fd; }

        size_t size() const { return _size; }

    private:
        const std::string _filename;
        const bool        _delete_on_close;

        int    _fd;
        size_t _size;
    };


    class MMappedFile : public OpenedFile {
    public:
        MMappedFile(const std::string& filename,
                    bool unmap_on_close=true,
                    bool delete_on_close=false)
            : OpenedFile(filename, delete_on_close)
            , _address(mmap(0, size(), PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd(), 0)) {

            if (_address == reinterpret_cast<void*>(-1) && size() != 0) {
                throw imhotep_error(std::string(__PRETTY_FUNCTION__)
                                    + std::string(": ") + std::string(strerror(errno)));
            }
        }

        MMappedFile(const std::string& filename, void* address)
            : OpenedFile(filename, -1)
            , _address(address)
        { }

        ~MMappedFile() {
            if (_unmap_on_close && _address != reinterpret_cast<void*>(-1)) {
                munmap(_address, size());
            }
        }

        MMappedFile(const MMappedFile& rhs)            = delete;
        MMappedFile& operator=(const MMappedFile& rhs) = delete;

        const char* begin() const {
            return size() != 0 ? reinterpret_cast<const char*>(_address) : 0;
        }

        const char* end() const { return begin() + size(); }

    private:
        void* _address        = 0;
        bool  _unmap_on_close = false;
    };

} // namespace imhotep

#endif
