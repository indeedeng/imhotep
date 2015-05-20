#pragma once

#include <vector>
#include <iterator>

#include <boost/iterator/iterator_facade.hpp>

namespace imhotep {

    template <typename iter_t>
    class interleaved_iterator
        : public boost::iterator_facade<interleaved_iterator<iter_t>,
                                         typename std::iterator_traits<iter_t>::value_type,
                                         boost::forward_traversal_tag> {
    public:
        typedef typename std::iterator_traits<iter_t>::reference ref_type;

        interleaved_iterator() { }

        template <typename i1, typename i2>
        interleaved_iterator(i1 iters_begin,
                             i1 iters_end,
                             i2 ends_begin,
                             i2 ends_end)
            :
                _iterators(iters_begin, iters_end),
                _ends(ends_begin, ends_end),
                current_iter(_iterators.begin()),
                current_end(_ends.begin())
        { }

    private:
        friend class boost::iterator_core_access;

        void increment()
        {
            static iter_t end;

            if (_iterators.empty()) {
                return;
            }

            (*current_iter) ++;

            if (*current_iter == *current_end) {
                current_iter = _iterators.erase(current_iter);
                current_end = _ends.erase(current_end);
            } else {
                current_iter ++;
                current_end ++;
            }

            if (current_iter == _iterators.end()) {
                current_iter = _iterators.begin();
                current_end = _ends.begin();
            }
        }

        bool equal(const interleaved_iterator& other) const
        {
            return _iterators == other._iterators;
        }

        const ref_type dereference() const { return **current_iter; }

        std::vector<iter_t> _iterators;
        std::vector<iter_t> _ends;
        typename std::vector<iter_t>::iterator current_iter;
        typename std::vector<iter_t>::iterator current_end;
    };

} // namespace imhotep
