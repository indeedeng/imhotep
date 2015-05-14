#ifndef INT_TERM_ITERATOR_HPP
#define INT_TERM_ITERATOR_HPP

#include <cassert>
#include <cstring>
#include <memory>
#include <string>

#include <boost/iterator/iterator_facade.hpp>

#include "term.hpp"
#include "var_int_view.hpp"

namespace imhotep {

    class int_term_iterator
        : public boost::iterator_facade<int_term_iterator, IntTerm const,
                                        boost::forward_traversal_tag> {
    public:
        int_term_iterator() { }

        int_term_iterator(const std::string& shard_dir, const std::string& name)
            : _view(std::make_shared<MMappedVarIntView>(shard_dir + "/fld-" + name + ".intterms")) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        void increment() {
            if (!_view->empty()) {
                const long id_delta(_view->read_varint<long>(_view->read()));
                const long offset_delta(_view->read_varint<long>(_view->read()));
                const long doc_freq(_view->read_varint<long>(_view->read()));
                _current = IntTerm(_current.id() + id_delta,
                                   _current.doc_offset() + offset_delta,
                                   doc_freq);
            }
            else {
                _view = std::make_shared<MMappedVarIntView>();
            }
        }

        bool equal(const int_term_iterator& other) const {
            return *_view == *other._view;
        }

        const IntTerm& dereference() const { return _current; }

        // !@# replace with intrusive?
        std::shared_ptr<MMappedVarIntView> _view = std::make_shared<MMappedVarIntView>();
        IntTerm                            _current;
    };
}

#endif
