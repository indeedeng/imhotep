#ifndef SPLIT_ITERATOR_HPP
#define SPLIT_ITERATOR_HPP

#include <fstream>
#include <memory>
#include <boost/iterator/iterator_facade.hpp>

#include "term.hpp"

namespace imhotep {

    class split_int_term_iterator
        : public boost::iterator_facade<split_int_term_iterator, IntTerm const,
                                        boost::forward_traversal_tag> {

    public:
        split_int_term_iterator() { }

        split_int_term_iterator(const std::string& split_file)
            : _ifs(std::make_shared<std::ifstream>(split_file.c_str())) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        void increment() {
            if (_ifs && _ifs->good()) {
                _ifs->read(reinterpret_cast<char*>(&_current), sizeof(IntTerm));
            }
            if (_ifs->eof()) _ifs.reset();
        }

        bool equal(const split_int_term_iterator& other) const {
            return
                (!_ifs && !other._ifs) ||
                (_ifs && other._ifs && *_ifs == *other._ifs);
        }

        const IntTerm& dereference() const { return _current; }

        std::shared_ptr<std::ifstream> _ifs;
        IntTerm                        _current;
    };

}

#endif
