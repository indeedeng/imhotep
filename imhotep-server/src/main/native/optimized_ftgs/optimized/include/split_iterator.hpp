#ifndef SPLIT_ITERATOR_HPP
#define SPLIT_ITERATOR_HPP

#include <iterator>
#include <memory>
#include <vector>
#include <boost/iterator/iterator_facade.hpp>

#include "split_view.hpp"
#include "term.hpp"

namespace imhotep {

    template <typename term_t>
    class SplitIterator
        : public boost::iterator_facade<SplitIterator<term_t>,
                                        term_t const,
                                        boost::forward_traversal_tag> {
    public:
        SplitIterator() { }

        SplitIterator(const SplitView& view)
            : _view(view) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        void increment() {
            if (!_view.empty()) {
                _current = _view.read<term_t>();
            }
        }

        bool equal(const SplitIterator& other) const {
            return (_view.empty() && other._view.empty()) || _view == other._view;
        }

        const term_t& dereference() const { return _current; }

        term_t decode();

        SplitView _view;
        term_t    _current;
    };
}

#endif
