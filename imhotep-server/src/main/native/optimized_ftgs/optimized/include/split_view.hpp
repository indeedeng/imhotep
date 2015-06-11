#ifndef SPLIT_VIEW_HPP
#define SPLIT_VIEW_HPP

#include <memory>

#include "term.hpp"

namespace imhotep {

    class SplitView {
    public:
        typedef std::pair<const char*, const char*> Buffer;

        SplitView(const char* begin = 0,
                  const char* end   = 0)
            : _begin(begin)
            , _end(end) {
            if (_begin > _end) {
                throw std::length_error(__PRETTY_FUNCTION__);
            }
        }

        bool operator==(const SplitView& rhs) const {
            return _begin == rhs._begin && _end == rhs._end;
        }

        bool empty() const { return _begin >= _end; }

        template <typename T>
        T read() {
            check_size(sizeof(T));

            const T* result(reinterpret_cast<const T*>(_begin));
            _begin += sizeof(T);
            return T(*result);
        }

        Buffer read_bytes(size_t length) {
            check_size(length);

            const Buffer result(_begin, _begin + length);
            _begin += length;
            return result;
        }

    private:
        void check_size(size_t size) {
            /* Note that this class maintains the invariant that _begin <= _end,
               therefore we need not worry about negative distances below. */
            if (_begin + size > _end) {
                throw std::length_error(__PRETTY_FUNCTION__);
            }
        }

        const char* _begin = 0;
        const char* _end   = 0;
    };


    template<>
    inline
    StringTerm SplitView::read<StringTerm>() {
        const size_t            id_size(read<size_t>());
        const SplitView::Buffer id(read_bytes(id_size));
        const int64_t           doc_offset(read<int64_t>());
        const int32_t           doc_freq(read<int32_t>());
        return StringTerm(std::string(id.first, id.second), doc_offset, doc_freq);
    }
}

#endif