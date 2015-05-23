#pragma once

#include <functional>
#include <vector>
#include <iterator>
#include <type_traits>

#include <boost/iterator/iterator_facade.hpp>

namespace imhotep {

    template<typename iter_type1, typename iter_type2>
    class ChainedIterator
        : public boost::iterator_facade<ChainedIterator<iter_type1, iter_type2>,
                                         typename std::iterator_traits<iter_type1>::value_type const,
                                         boost::forward_traversal_tag> {

        static_assert(std::is_same<typename std::iterator_traits<iter_type1>::value_type,
                                    typename std::iterator_traits<iter_type2>::value_type>::value,
                                     "Iterators must have the same value type! \n");

    public:
        typedef typename std::vector<std::pair<iter_type1,iter_type1>> t1_pairs_vector;
        typedef typename std::vector<std::pair<iter_type2,iter_type2>> t2_pairs_vector;
        typedef typename std::iterator_traits<iter_type1>::value_type element_type;
        typedef typename std::function<element_type(int32_t)> generator_type;

        ChainedIterator() { }

        ChainedIterator(std::pair<t1_pairs_vector, t2_pairs_vector> p,
                        generator_type&& f1,
                        generator_type&& f2) :
                _t1_iters(p.first),
                _t2_iters(p.second),
                _prefix_func(f1),
                _suffix_func(f2)
        {
            if (_t1_iters && !_t1_iters.empty()) {
                _current_t1_iter = _t1_iters[0].begin();
                _current_t1_end = _t1_iters[0].end();
            }
            if (_t2_iters && !_t2_iters.empty()) {
                _current_t2_iter = _t2_iters[0].begin();
                _current_t2_end = _t2_iters[0].end();
            }

            begin = true;
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        bool inc_core(auto& it_curr, auto& it_end)
        {
            if (begin) {
                _current_val = _prefix_func(_current_iter_num);
                begin = false;
                return true;
            }

            if (end) {
                _current_val = _suffix_func(_current_iter_num);
                end = false;
                return false;
            }

            _current_val = **it_curr;

            (*it_curr) ++;
            if (*it_curr == *it_end) {
                end = true;
            }
            return true;
        }

        template<typename iter_t>
        bool inc_type(iter_t& iters, auto& it_curr, auto& it_end)
        {
            bool current_iter_has_more = inc_core(it_curr, it_end);

            if (current_iter_has_more) {
                return true;
            }

            iters.erase(iters.begin() + _current_iter_num);
            if (!iters.empty()) {
                std::tie(it_curr, it_end) = iters[0];
                _current_iter_num ++;
                begin = true;
                return true;
            }
            return false;
        }

        void increment()
        {
            if (_current_iter_num < _t1_iters.size()) {
                inc_type(_t1_iters, _current_t1_iter, _current_t1_end);
            } else {
                inc_type(_t2_iters, _current_t2_iter, _current_t2_end);
            }
        }

        bool equal(const ChainedIterator& other) const
        {
            return (_t1_iters == other._t1_iters)
                    && (_t2_iters == other._t2_iters)
                    && (begin == other.begin)
                    && (end == other.end);
        }

        const element_type& dereference() const { return _current_val; }

        t1_pairs_vector _t1_iters;
        t2_pairs_vector _t2_iters;
        iter_type1 _current_t1_iter;
        iter_type1 _current_t1_end;
        iter_type2 _current_t2_iter;
        iter_type2 _current_t2_end;
        int32_t _current_iter_num = 0;
        generator_type _prefix_func;
        generator_type _suffix_func;
        element_type _current_val = 0;
        bool begin = false;
        bool end = false;
    };

} // namespace imhotep
