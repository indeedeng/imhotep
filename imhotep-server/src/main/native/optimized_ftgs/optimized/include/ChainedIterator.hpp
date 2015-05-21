#pragma once

#include <functional>
#include <vector>
#include <iterator>

#include <boost/iterator/iterator_facade.hpp>

namespace imhotep {

    template<typename iter_t>
    class ChainedIterator
        : public boost::iterator_facade<ChainedIterator<iter_t>,
                                         typename std::iterator_traits<iter_t>::value_type const,
                                         boost::forward_traversal_tag> {
    public:
        typedef typename std::iterator_traits<iter_t>::value_type element_type;
        typedef typename std::function<element_type(int32_t)> generator_type;

        ChainedIterator() { }

        template<typename i1, typename i2>
        ChainedIterator(i1 iters_begin,
                        i1 iters_end,
                        i2 ends_begin,
                        i2 ends_end,
                        generator_type&& f1,
                        generator_type&& f2) :
                _iterators(iters_begin, iters_end),
                _ends(ends_begin, ends_end),
                _current_iter(_iterators.begin()),
                _current_end(_ends.begin()),
                _prefix_func(f1),
                _suffix_func(f2)
        {
            if (!_iterators.empty()) {
                begin = true;
                increment();
            }
        }

    private:
        friend class boost::iterator_core_access;

        void increment()
        {
            if (begin) {
                _current_val = _prefix_func(_current_iter_num);
                begin = false;
                return;
            }

            if (end) {
                _current_val = _suffix_func(_current_iter_num);
                end = false;
                if (!_iterators.empty()) {
                    _current_iter_num ++;
                    begin = true;
                }
                return;
            }

            _current_val = **_current_iter;

            (*_current_iter) ++;
            if (*_current_iter == *_current_end) {
                _current_iter = _iterators.erase(_current_iter);
                _current_end = _ends.erase(_current_end);
                end = true;
            }
        }

        bool equal(const ChainedIterator& other) const
        {
            return (_iterators == other._iterators)
                    && (begin == other.begin)
                    && (end == other.end);
        }

        const element_type& dereference() const { return _current_val; }

        std::vector<iter_t> _iterators;
        std::vector<iter_t> _ends;
        typename std::vector<iter_t>::iterator _current_iter;
        typename std::vector<iter_t>::iterator _current_end;
        int32_t _current_iter_num = 0;
        generator_type _prefix_func;
        generator_type _suffix_func;
        element_type _current_val = 0;
        bool begin = false;
        bool end = false;
    };

} // namespace imhotep
