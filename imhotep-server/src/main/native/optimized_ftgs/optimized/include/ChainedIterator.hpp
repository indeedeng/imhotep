#pragma once

#include <functional>
#include <vector>
#include <iterator>
#include <type_traits>

namespace imhotep {

    template<typename jIter_type1, typename jIter_type2>
    class ChainedIterator {

        static_assert(std::is_same<typename jIter_type1::value_type,
                                    typename jIter_type2::value_type>::value,
                                     "Iterators must have the same value type! \n");

    public:
        typedef typename jIter_type1::value_type value_type;
        typedef typename std::vector<jIter_type1> t1_vector_t;
        typedef typename std::vector<jIter_type2> t2_vector_t;
        typedef typename std::function<value_type(int32_t)> generator_type;

        ChainedIterator() { }

        ChainedIterator(t1_vector_t t1_vec,
                        t2_vector_t t2_vec,
                        generator_type&& f1,
                        generator_type&& f2) :
                _t1_iters(t1_vec),
                _t2_iters(t2_vec),
                _prefix_func(f1),
                _suffix_func(f2)
        {
            if (_t1_iters && !_t1_iters.empty()) {
                _current_t1_iter = _t1_iters[0];
            }
            if (_t2_iters && !_t2_iters.empty()) {
                _current_t2_iter = _t2_iters[0];
            }

            begin = true;
        }

    private:
        template<typename jIter_t>
        bool inc_core(jIter_t& it_curr, value_type& data)
        {
            if (begin) {
                data = _prefix_func(_current_iter_num);
                begin = false;
                return true;
            }

            if (end) {
                data = _suffix_func(_current_iter_num);
                end = false;
                return false;
            }

            it_curr.next(data);
            if (!it_curr.hasNext()) {
                end = true;
            }
            return true;
        }

        template<typename jIter_t>
        bool inc_type(std::vector<jIter_t>& iters, jIter_t& it_curr, value_type& data)
        {
            bool current_iter_has_more = inc_core(it_curr, data);

            if (current_iter_has_more) {
                return true;
            }

            if (_current_iter_num < iters.size()) {
                it_curr = iters[0];
                _current_iter_num ++;
                begin = true;
                return true;
            }
            return false;
        }

        void next(value_type& data)
        {
            if (_current_iter_num < _t1_iters.size()) {
                inc_type(_t1_iters, _current_t1_iter, data);
            } else {
                inc_type(_t2_iters, _current_t2_iter, data);
            }
        }

        bool hasNext() const
        {
            return _current_iter_num < (_t1_iters.size() + _t2_iters.size());
        }

        t1_vector_t _t1_iters;
        t2_vector_t _t2_iters;
        jIter_type1 _current_t1_iter;
        jIter_type2 _current_t2_iter;
        int32_t _current_iter_num = 0;
        generator_type _prefix_func;
        generator_type _suffix_func;
        bool begin = false;
        bool end = false;
    };

} // namespace imhotep
