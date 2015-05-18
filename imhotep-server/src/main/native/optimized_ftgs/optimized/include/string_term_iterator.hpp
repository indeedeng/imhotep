#ifndef STRING_TERM_ITERATOR_HPP
#define STRING_TERM_ITERATOR_HPP

#include <cassert>
#include <cstring>
#include <memory>
#include <string>

#include <boost/iterator/iterator_facade.hpp>

#include "term.hpp"
#include "var_int_view.hpp"

namespace imhotep {

    class string_term_iterator
        : public boost::iterator_facade<string_term_iterator, StringTerm const,
                                        boost::forward_traversal_tag> {
    public:
        string_term_iterator() { }

        string_term_iterator(const std::string& shard_dir, const std::string& name)
            : _view(std::make_shared<MMappedVarIntView>(shard_dir + "/fld-" + name + ".strterms")) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        /* !@# This is currently written for comfort, not speed. Further optimization
            might be required... */
        void increment() {
            if (!_view->empty()) {
                const long erase(_view->read_varint<long>(_view->read()));
                const long append(_view->read_varint<long>(_view->read()));
                _buffer.erase(_buffer.size() - erase, erase);
                for (size_t count(0); count < size_t(append); ++count) {
                    _buffer.push_back(_view->read());
                }

                const long offset_delta(_view->read_varint<long>(_view->read()));
                const long doc_freq(_view->read_varint<long>(_view->read()));
                _current = StringTerm(_buffer,
                                      _current.doc_offset() + offset_delta,
                                      doc_freq);
            }
            else {
                _view = std::make_shared<MMappedVarIntView>();
            }
        }

        bool equal(const string_term_iterator& other) const {
            return *_view == *other._view;
        }

        const StringTerm& dereference() const { return _current; }

        // !@# replace with intrusive?
        std::shared_ptr<MMappedVarIntView> _view = std::make_shared<MMappedVarIntView>();

        StringTerm  _current;
        std::string _buffer;
    };
}

#endif
