#pragma once

#include <vector>
#include <iterator>

#include <boost/iterator/iterator_facade.hpp>

namespace imhotep {

    template<typename iter_t, class prefix_f, class prefix_arg, class suffix_f, class suffix_arg>
    class ChainedIterator
        : public boost::iterator_facade<ChainedIterator<iter_t>,
                                         typename std::iterator_traits<iter_t>::value_type,
                                         boost::forward_traversal_tag> {
    public:
        typedef typename std::iterator_traits<iter_t>::value_type element_type;

        ChainedIterator() { }

        template <typename i1, typename i2>
        ChainedIterator(i1 iters_begin,
                             i1 iters_end,
                             i2 ends_begin,
                             i2 ends_end,
                             prefix_f&& f1,
                             prefix_arg&& arg1,
                             suffix_f&& f2,
                             suffix_arg&& arg2)
            :
                _iterators(iters_begin, iters_end),
                _ends(ends_begin, ends_end),
                _current_iter(_iterators.begin()),
                _current_end(_ends.begin()),
                _prefix_func(f)
        { }

    private:
        friend class boost::iterator_core_access;

        void increment()
        {
            if (_iterators.empty()) {
                return;
            }

            (*_current_iter) ++;

            if (*_current_iter == *_current_end) {
                _current_iter = _iterators.erase(_current_iter);
                _current_end = _ends.erase(_current_end);
                _current_iter_num ++;
            }
        }

        bool equal(const ChainedIterator& other) const
        {
            return _iterators == other._iterators;
        }

        const element_type& dereference() const { return **_current_iter; }

        std::vector<iter_t> _iterators;
        std::vector<iter_t> _ends;
        typename std::vector<iter_t>::iterator _current_iter;
        typename std::vector<iter_t>::iterator _current_end;
        int32_t _current_iter_num = 0;
        std::function<element_type(prefix_arg, int32_t)> _prefix_func;
        std::function<element_type(suffix_arg, int32_t)> _suffix_func;
    };

} // namespace imhotep
