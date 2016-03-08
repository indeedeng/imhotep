#ifndef SPLIT_FILE_HPP
#define SPLIT_FILE_HPP

#include "split_view.hpp"

#include <mutex>
#include <string>

namespace imhotep {

    /** Manages a temporary split file in the context of an FTGS
        operation. There are two consumers of this, a writer (Splitter) and a
        reader (SplitView), and they need to share the same fd so that we can
        reliably cleanup their underlying temp file in crash scenarios (see
        IMTEPD-199).
     */
    class SplitFile {
    public:
        SplitFile(const std::string& filename);
        SplitFile(const SplitFile&) = delete;
        ~SplitFile();

        SplitFile& operator=(const SplitFile&) = delete;

        const std::string& filename() const { return _filename; }
        int                      fd() const { return _fd;       }

        SplitView split_view();

        std::string to_string() const;

    private:
        size_t file_size() const;

        void map();

        bool mapped() { return _addr != 0 && _length != 0; }

        const std::string _filename;
        const int         _fd;

        void * _addr   = 0;     // address of mapping, if any
        size_t _length = 0;     // length of mapping, if any

        std::mutex _map_mutex;
    };
}

#endif
