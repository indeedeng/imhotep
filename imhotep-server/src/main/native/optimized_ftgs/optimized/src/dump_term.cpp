#define restrict __restrict__

#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#define restrict __restrict__
extern "C" {
#include "imhotep_native.h"
#include "local_session.h"
}

#include "varintdecode.h"

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
    Buffer(const Buffer& rhs) = delete;

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

class IntFieldView {
    Buffer _terms;
    Buffer _docs;

    long _min = numeric_limits<long>::max();
    long _max = numeric_limits<long>::min();

    long _min_doc_id = numeric_limits<long>::max();
    long _max_doc_id = numeric_limits<long>::min();

    long _max_term_doc_freq = 0;

    size_t _n_terms = 0;

public:
    class TermIterator {
        View _view;
        long _term   = 0;
        long _offset = 0;
    public:
        typedef tuple<long, long, long> Tuple; // term, offset, doc frequency

        TermIterator(const View& view) : _view(view) { }
        bool has_next() { return !_view.empty(); }

        Tuple next() {
            const long term_delta(_view.read_varint<long>(_view.read()));
            const long offset_delta(_view.read_varint<long>(_view.read()));
            const long doc_freq(_view.read_varint<long>(_view.read()));
            assert(doc_freq > 0);
            _term   += term_delta;
            _offset += offset_delta;
            return Tuple(_term, _offset, doc_freq);
        }
    };

    IntFieldView(const string& shard_dir, const string& name)
        : _terms(shard_dir + "/fld-" + name + ".intterms")
        , _docs(shard_dir + "/fld-" + name + ".intdocs") {
        TermIterator it(term_iterator());
        while (it.has_next()) {
            const TermIterator::Tuple next(it.next());
            const long term(get<0>(next));
            const long offset(get<1>(next));
            const long doc_freq(get<2>(next));
            _min = std::min(term, _min);
            _max = std::max(term, _max);
            _max_term_doc_freq = std::max(doc_freq, _max_term_doc_freq);

            View doc_ids_view(_docs.begin() + offset, _docs.end());
            scrape_doc_ids(doc_ids_view, doc_freq);
            ++_n_terms;
        }
    }

    long min() const { return _min; }
    long max() const { return _max; }

    long min_doc_id() const { return _min_doc_id; }
    long max_doc_id() const { return _max_doc_id; }

    long max_term_doc_freq() const { return _max_term_doc_freq; }

    int n_rows() const {
        assert(max_doc_id() > min_doc_id());
        return max_doc_id() - min_doc_id() + 1;
    }

    size_t n_terms() const { return _n_terms; }

    TermIterator term_iterator() const { return TermIterator(View(_terms.begin(), _terms.end())); }

    const char* doc_id_stream(long offset) const { return _docs.begin() + offset; };

    void pack(packed_table_t* table, int col, bool group_by_me=false) const {
        TermIterator it(term_iterator());
        while (it.has_next()) {
            const TermIterator::Tuple next(it.next());
            const long term(get<0>(next));
            const long offset(get<1>(next));
            const long doc_freq(get<2>(next));
            View doc_ids_view(_docs.begin() + offset, _docs.end());
            pack(doc_ids_view, doc_freq, table, term, col, group_by_me);
        }
    }

private:
    void scrape_doc_ids(const View& view, long term_doc_freq) {
        View                  window(view.begin(), view.end());
        array<uint32_t, 4099> buffer;
        uint32_t              prev(0);
        uint64_t              remaining(term_doc_freq);
        while (remaining > 0) {
            const uint64_t length(std::min(remaining, buffer.size()));
            const size_t   bytes(masked_vbyte_read_loop_delta((uint8_t*) window.begin(), buffer.data(), length, prev));
            remaining -= length;
            window = View(window.begin() + bytes, window.end());
            prev = buffer[length - 1];
            for (size_t i_doc_id(0); i_doc_id < length; ++i_doc_id) {
                const long doc_id(buffer[i_doc_id]);
                _min_doc_id = std::min(_min_doc_id, doc_id);
                _max_doc_id = std::max(_max_doc_id, doc_id);
            }
        }
    }

    void pack(const View& view, long term_doc_freq, packed_table_t* table,
              int64_t term, int col, bool group_by_me) const {
        View                window(view.begin(), view.end());
        array<uint32_t, 16> row_ids;
        uint32_t            prev(0);
        uint64_t            remaining(term_doc_freq);
        array<int64_t, 16>  col_vals;
        array<int32_t, 16>  group_vals;
        fill(col_vals.begin(), col_vals.end(), term);
        if (group_by_me) fill(group_vals.begin(), group_vals.end(), term);
        while (remaining > 0) {
            const uint64_t length(std::min(remaining, row_ids.size()));
            const size_t   bytes(masked_vbyte_read_loop_delta((uint8_t*) window.begin(), row_ids.data(), length, prev));
            remaining -= length;
            window = View(window.begin() + bytes, window.end());
            prev = row_ids[length - 1];
            packed_table_batch_set_col(table, (int*) row_ids.data(), length, col_vals.data(), col);
            if (group_by_me) {
                packed_table_batch_set_group(table, (int*) row_ids.data(), length, group_vals.data());
            }
        }
    }
};

ostream&
operator<<(ostream& os, const IntFieldView& view) {
    os << "min: "                << setw(10) << view.min()
       << " max: "               << setw(10) << view.max()
       << " min_doc_id: "        << setw(10) << view.min_doc_id()
       << " max_doc_id: "        << setw(10) << view.max_doc_id()
       << " n_terms: "           << setw(10) << view.n_terms()
       << " max_term_doc_freq: " << setw(10) << view.max_term_doc_freq();
    return os;
}

int main(int argc, char* argv[])
{
    simdvbyteinit();

    if (argc < 3) {
        cerr << "usage: dump_term <shard dir> <field> (<field>...)" << endl;
        exit(1);
    }
    string shard_dir(argv[1]);
    vector<string> fields;
    for (size_t i_argv(2); i_argv < argc; ++i_argv) { fields.push_back(argv[i_argv]); }

    vector<shared_ptr<IntFieldView>> field_views;
    for (auto field: fields) {
        field_views.push_back(make_shared<IntFieldView>(shard_dir, field));
        cout << setw(10) << field << " " << **field_views.rbegin() << endl;
    }

    int n_rows(0);
    size_t n_cols(fields.size());
    vector<int64_t> col_mins(n_cols), col_maxes(n_cols);
    vector<int32_t> sizes(n_cols), vec_nums(n_cols), offsets(n_cols);
    for (size_t col(0); col < n_cols; ++col) {
        /* naively assign fields to slices */
        IntFieldView& field_view(*field_views[col]);
        n_rows         = std::max(n_rows, field_view.n_rows());
        col_mins[col]  = field_view.min();
        col_maxes[col] = field_view.max();
        sizes[col]     = 8;
        vec_nums[col]  = 1 + col / 2;
        offsets[col]   = col % 2;
    }

    packed_table_t* table(packed_table_create(n_rows,
                                              col_mins.data(), col_maxes.data(),
                                              sizes.data(), vec_nums.data(), offsets.data(),
                                              n_cols));
    for (size_t col(0); col < n_cols; ++col) {
        field_views[col]->pack(table, col, col == 0);
    }

    array <int, 1> socket_file_desc{{3}};
    struct worker_desc  worker;
    worker_init(&worker, 1, n_rows, n_cols, socket_file_desc.data(), 1);

    uint8_t shard_order[] = {0};
    struct session_desc session;
    session_init(&session, n_rows, n_cols, shard_order, 1);

    array <int, 1> shard_handles;
    shard_handles[0] = register_shard(&session, table);

    IntFieldView::TermIterator it(field_views[0]->term_iterator());
    while (it.has_next()) {
        IntFieldView::TermIterator::Tuple next(it.next());
        const long offset(get<1>(next));
        const long doc_freq(get<2>(next));
        array<long, 1> addresses{{(long) field_views[0]->doc_id_stream(offset)}};
        array<int, 1> docs_in_term{{(int) doc_freq}};

        struct runtime_err error;
        memset(&error, 0, sizeof(error));

        run_tgs_pass(&worker,
                     &session,
                     TERM_TYPE_INT,
                     1,
                     NULL,
                     addresses.data(),
                     docs_in_term.data(),
                     shard_handles.data(),
                     1,
                     socket_file_desc[0],
                     &error);

        assert(error.code == 0);
    }

    packed_table_destroy(table);
}
