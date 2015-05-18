#ifndef SPLIT_ITERATOR_HPP
#define SPLIT_ITERATOR_HPP

#include <fstream>
#include <memory>
#include <vector>
#include <boost/iterator/iterator_facade.hpp>

#include "term.hpp"

namespace imhotep {

    template <typename term_t>
    struct split_iterator_traits {
        typedef void buffer_t;
    };

    template <typename term_t>
    class split_iterator
        : public boost::iterator_facade<split_iterator<term_t>,
                                        term_t const,
                                        boost::forward_traversal_tag> {

    public:
        split_iterator() { }

        split_iterator(const std::string& split_file)
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

        bool equal(const split_iterator& other) const {
            return
                (!_ifs && !other._ifs) ||
                (_ifs && other._ifs && *_ifs == *other._ifs);
        }

        const term_t& dereference() const { return _current; }

        term_t decode(std::istream& is);

        std::shared_ptr<std::ifstream> _ifs;

        term_t _current;

        typename split_iterator_traits<term_t>::buffer_t _id_buffer;
    };

    template <> struct split_iterator_traits<IntTerm> {
        struct Unused { };
        typedef Unused buffer_t;
    };

    template <> struct split_iterator_traits<StringTerm> {
        typedef std::vector<char> buffer_t;
    };

    template<>
    IntTerm split_iterator<IntTerm>::decode(std::istream& is)
    {
        IntTerm result;
        is.read(reinterpret_cast<char*>(&result), sizeof(IntTerm));
        return result;
    }

    template<>
    StringTerm split_iterator<StringTerm>::decode(std::istream& is)
    {
        size_t   id_size(0);
        uint64_t doc_offset(0);
        uint64_t doc_freq(0);
        is.read(reinterpret_cast<char*>(&id_size), sizeof(id_size));
        _id_buffer.resize(id_size);
        is.read(_id_buffer.data(), id_size);
        is.read(reinterpret_cast<char *>(&doc_offset), sizeof(doc_offset));
        is.read(reinterpret_cast<char *>(&doc_freq), sizeof(doc_freq));
        return StringTerm(std::string(_id_buffer.begin(), _id_buffer.end()),
                          doc_offset, doc_freq);
    }

    typedef split_iterator<IntTerm>    split_int_term_iterator;
    typedef split_iterator<StringTerm> split_string_term_iterator;
}

#endif
