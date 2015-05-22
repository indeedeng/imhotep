#ifndef SPLIT_ITERATOR_HPP
#define SPLIT_ITERATOR_HPP

#include <fstream>
#include <memory>
#include <vector>
#include <boost/iterator/iterator_facade.hpp>

#include "term.hpp"

namespace imhotep {

    template <typename term_t>
    struct SplitIteratorTraits {
        typedef void buffer_t;
    };

    template <typename term_t>
    class SplitIterator
        : public boost::iterator_facade<SplitIterator<term_t>,
                                        term_t const,
                                        boost::forward_traversal_tag> {

    public:
        SplitIterator() { }

        SplitIterator(const std::string& split_file)
            : _ifs(std::make_shared<std::ifstream>(split_file.c_str())) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        void increment() {
            if (_ifs && _ifs->good()) {
                _current = decode(*_ifs);
            }
            if (_ifs->eof()) _ifs.reset();
        }

        bool equal(const SplitIterator& other) const {
            return
                (!_ifs && !other._ifs) ||
                (_ifs && other._ifs && *_ifs == *other._ifs);
        }

        const term_t& dereference() const { return _current; }

        term_t decode(std::istream& is);

        std::shared_ptr<std::ifstream> _ifs;

        term_t _current;

        typename SplitIteratorTraits<term_t>::buffer_t _id_buffer;
    };

    template <> struct SplitIteratorTraits<IntTerm> {
        struct Unused { };
        typedef Unused buffer_t;
    };

    template <> struct SplitIteratorTraits<StringTerm> {
        typedef std::vector<char> buffer_t;
    };

}

#endif
