#ifndef SPLIT_ITERATOR_HPP
#define SPLIT_ITERATOR_HPP

#include <iterator>
#include <memory>
#include <vector>
#include <boost/iterator/iterator_facade.hpp>

#include "mmapped_file.hpp"
#include "term.hpp"

namespace imhotep {

    class SplitView {
    public:
        typedef std::pair<const char*, const char*> Buffer;

        SplitView(const char* begin = 0,
                  const char* end   = 0)
            : _begin(begin)
            , _end(end)
        { }

        bool operator==(const SplitView& rhs) const {
            return _begin == rhs._begin && _end == rhs._end;
        }

        bool empty() const { return _begin >= _end; }

        template <typename T>
        T read() {
            /*
            if (sizeof(T) > std::distance(_begin, _end)) {
                throw std::length_error(__PRETTY_FUNCTION__);
                }*/
            const T* result(reinterpret_cast<const T*>(_begin));
            _begin += sizeof(T);
            return T(*result);
        }

        Buffer read_bytes(size_t length) {
            /*
            if (length > std::distance(_begin, _end)) {
                throw std::length_error(__PRETTY_FUNCTION__);
                }*/
            const Buffer result(_begin, _begin + length);
            _begin += length;
            return result;
        }

    private:
        const char* _begin = 0;
        const char* _end   = 0;
    };


    template <typename term_t>
    class SplitIterator
        : public boost::iterator_facade<SplitIterator<term_t>,
                                        term_t const,
                                        boost::forward_traversal_tag> {

    public:
        SplitIterator() { }

        SplitIterator(const std::string& split_file)
            : _file(std::make_shared<MMappedFile>(split_file))
            , _view(_file->begin(), _file->end()) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        void increment() {
            if (!_view.empty()) {
                _current = decode();
            }
        }

        bool equal(const SplitIterator& other) const {
            return (_view.empty() && other._view.empty()) || _view == other._view;
        }

        const term_t& dereference() const { return _current; }

        term_t decode();

        std::shared_ptr<MMappedFile> _file;
        SplitView                    _view;

        term_t _current;
    };

}

#endif
