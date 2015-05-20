#ifndef TERM_ITERATOR_HPP
#define TERM_ITERATOR_HPP

#include <cassert>
#include <cstring>
#include <memory>
#include <cstdint>
#include <string>

#include <boost/iterator/iterator_facade.hpp>

#include "term.hpp"
#include "var_int_view.hpp"

namespace imhotep {

    template <typename term_t>
    struct TermIteratorTraits {
        typedef void buffer_t;
    };

    template <typename term_t>
    class TermIterator
        : public boost::iterator_facade<TermIterator<term_t>, term_t const,
                                        boost::forward_traversal_tag> {
    public:
        TermIterator() { }

        TermIterator(const std::string& filename,
                     int64_t docid_base=0)
            : _view(std::make_shared<MMappedVarIntView>(filename))
            , _docid_base(docid_base) {
            increment();
        }

        TermIterator(const char* begin, const char* end,
                     int64_t docid_base=0)
            : _view(std::make_shared<VarIntView>(begin, end))
            , _docid_base(docid_base) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        void increment();

        bool equal(const TermIterator& other) const { return *_view == *other._view; }

        const term_t& dereference() const { return _current; }

        std::shared_ptr<VarIntView> _view = std::make_shared<VarIntView>();

        int64_t _docid_base = 0;

        term_t _current;

        typename TermIteratorTraits<term_t>::buffer_t _id_buffer;
    };

    template <> struct TermIteratorTraits<IntTerm> {
        struct Unused { };
        typedef Unused buffer_t;
    };

    template <> struct TermIteratorTraits<StringTerm> {
        typedef std::string buffer_t;
    };

    template<>
    void TermIterator<IntTerm>::increment() {
        if (!_view->empty()) {
            const int64_t id_delta(_view->read_varint<int64_t>(_view->read()));
            const int64_t offset_delta(_view->read_varint<int64_t>(_view->read()));
            const int64_t doc_freq(_view->read_varint<int64_t>(_view->read()));
            _current = IntTerm(_current.id() + id_delta,
                               _docid_base + _current.doc_offset() + offset_delta,
                               doc_freq);
        }
        else {
            _view = std::make_shared<MMappedVarIntView>();
        }
    }

    template<>
    void TermIterator<StringTerm>::increment() {
        if (!_view->empty()) {
            const int64_t erase(_view->read_varint<int64_t>(_view->read()));
            const int64_t append(_view->read_varint<int64_t>(_view->read()));
            _id_buffer.erase(_id_buffer.size() - erase, erase);
            for (size_t count(0); count < size_t(append); ++count) {
                _id_buffer.push_back(_view->read());
            }

            const int64_t offset_delta(_view->read_varint<int64_t>(_view->read()));
            const int64_t doc_freq(_view->read_varint<int64_t>(_view->read()));
            _current = StringTerm(_id_buffer,
                                  _docid_base + _current.doc_offset() + offset_delta,
                                  doc_freq);
        }
        else {
            _view = std::make_shared<MMappedVarIntView>();
        }
    }

    typedef TermIterator<IntTerm>    IntTermIterator;
    typedef TermIterator<StringTerm> StringTermIterator;

} // namespace imhotep

#endif
