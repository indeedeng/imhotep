#include <cassert>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace std;

size_t file_size(int fd)
{
    struct stat buf;
    int rc(fstat(fd, &buf));
    if (rc != 0) throw std::runtime_error(strerror(errno));
    return buf.st_size;
}

class View
{
protected:
    const char* _begin  = 0;
    const char* _end    = 0;

public:
    View(const char* begin=0, const char* end=0)
        : _begin(begin) , _end(end)
    { }

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

class Buffer : public View
{
    int         _fd     = 0;
    size_t      _length = 0;
    void*       _mapped = 0;

public:
    Buffer(const string& filename)
        : _fd(open(filename.c_str(), O_RDONLY)) {
        if (_fd <= 0) {
            throw std::runtime_error("cannot open file: " + filename);
        }

        _length = file_size(_fd);
        _mapped = mmap(0, _length, PROT_READ, MAP_PRIVATE | MAP_POPULATE, _fd, 0);
        if (_mapped == reinterpret_cast<void*>(-1)) {
            throw std::runtime_error("cannot mmap: " + string(strerror(errno)));
        }

        _begin = (const char *) _mapped;
        _end   = _begin + _length;
    }

    ~Buffer() {
        munmap(_mapped, _length);
        close(_fd);
    }

};

int main(int argc, char* argv[])
{
    string int_terms_file(string(argv[1]) + ".intterms");
    string int_docs_file(string(argv[1]) + ".intdocs");

    long last_term(0);
    Buffer term_buffer(int_terms_file);
    Buffer docs_buffer(int_docs_file);
    while (!term_buffer.empty()) {
        const uint8_t first(term_buffer.read());
        const long term_delta(term_buffer.read_varint<long>(first));
        const long offset_delta(term_buffer.read_varint<long>(term_buffer.read()));
        const long last_term_doc_freq(term_buffer.read_varint<long>(term_buffer.read()));
        last_term += term_delta;
        cout << "term: " << last_term << endl;
        cerr << "last_term: " << last_term
             << " term_delta: " << term_delta
             << " offset_delta: " << offset_delta
             << " last_term_doc_freq: " << last_term_doc_freq
             << endl;
        View term_docs_view(docs_buffer.begin() + offset_delta, docs_buffer.end());
        long doc_id(0);
        for (size_t i_doc(0); i_doc < last_term_doc_freq; ++i_doc) {
            const int32_t doc_id_delta(term_docs_view.read_varint<int32_t>(term_docs_view.read()));
            doc_id += doc_id_delta;
            cout << doc_id << endl;
        }
    }
}
