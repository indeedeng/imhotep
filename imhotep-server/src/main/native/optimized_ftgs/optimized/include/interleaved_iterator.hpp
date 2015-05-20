#pragma once

#include <vector>
#include <queue>
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

        template <typename iterator>
        interleaved_iterator(iterator begin, iterator end)
        {
            while(begin != end) {
                _iterators.push(*begin);
                ++ begin;
            }
        }

    private:
        friend class boost::iterator_core_access;

        void increment()
        {
            static iter_t end;

            if (_iterators.empty()) {
                return;
            }

            while (!_iterators.empty()
                    && _iterators.front() == end)
                _iterators.pop();

            if (!_iterators.empty()) {
                iter_t it(_iterators.front());
                ++it;
                _iterators.pop();
                if (it != end)
                    _iterators.push(it);
            }
        }

        bool equal(const interleaved_iterator& other) const
        {
            return _iterators == other._iterators;
        }

        const ref_type dereference() const
        {
            static typename std::iterator_traits<iter_t>::value_type end;

            if (!_iterators.empty())
                return *_iterators.front();

            return end;
        }

        std::queue<iter_t> _iterators;
       // typename std::vector<iter_t>::iterator current_iter;
    };

} // namespace imhotep
