#ifndef MMAPPED_FILE_HPP
#define MMAPPED_FILE_HPP

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <stdexcept>
#include <string>

namespace imhotep {

    class OpenedFile {
    public:
        OpenedFile(const std::string& filename, bool delete_on_close=false)
            : _filename(filename)
            , _delete_on_close(delete_on_close)
            , _fd(open(filename.c_str(), O_RDONLY)) {

            if (_fd <= 0) {
                throw std::runtime_error("cannot open file: " + filename +
                                         " " + std::string(strerror(errno)));
            }

            struct stat buf;
            int rc(fstat(_fd, &buf));
            if (rc != 0) {
                close(_fd);
                if (_delete_on_close) {
                    unlink(_filename.c_str());
                }
                throw std::runtime_error(strerror(errno));
            }
            _size = buf.st_size;
        }

        ~OpenedFile() {
            close(_fd);
            if (_delete_on_close) {
                unlink(_filename.c_str());
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
        MMappedFile(const std::string& filename, bool delete_on_close=false)
            : OpenedFile(filename, delete_on_close)
            , _address(mmap(0, size(), PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd(), 0)) {

            if (_address == reinterpret_cast<void*>(-1)) {
                throw std::runtime_error(std::string(__PRETTY_FUNCTION__)
                                         + std::string(": ") + std::string(strerror(errno)));
            }
        }

        ~MMappedFile() { munmap(_address, size()); }

        MMappedFile(const MMappedFile& rhs)            = delete;
        MMappedFile& operator=(const MMappedFile& rhs) = delete;

        const char* begin() const { return reinterpret_cast<const char*>(_address); }
        const char*   end() const { return begin() + size();                        }
    private:
        void* _address = 0;
    };

} // namespace imhotep

#endif
