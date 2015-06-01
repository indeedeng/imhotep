#ifndef CHAINED_ITERATOR_HPP
#define CHAINED_ITERATOR_HPP

#include <utility>
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
        typedef typename std::function<value_type(void)> end_tag_gen_type;

        ChainedIterator() { }

        ChainedIterator(t1_vector_t t1_vec,
                        t2_vector_t t2_vec,
                        generator_type f1,
                        generator_type f2,
                        end_tag_gen_type f3) :
                _t1_iters(t1_vec),
                _t2_iters(t2_vec),
                _prefix_func(f1),
                _suffix_func(f2),
                _end_func(f3),
                begin(true)
        { }


        void next(value_type& data)
        {
            if (_next_iter_num < _t1_iters.size()) {
                _next_iter_num = inc_type(_t1_iters, _next_iter_num, data);
            } else if (_next_iter_num < (_t1_iters.size() + _t2_iters.size())) {
                _next_iter_num = inc_type(_t2_iters, _next_iter_num - _t1_iters.size(), data)
                        + _t1_iters.size();
            } else {
                data = _end_func();
                _end_func_run = true;
            }
        }

        bool hasNext() const
        {
            return (_next_iter_num < (_t1_iters.size() + _t2_iters.size())) || !_end_func_run;
        }

    private:
        template<typename jIter_t>
        bool inc_core(jIter_t& it_curr, value_type& data)
        {
            if (begin) {
                data = _prefix_func(_next_iter_num);
                begin = false;
                return true;
            }

            if (it_curr.hasNext()) {
                it_curr.next(data);
                return true;
            }

            data = _suffix_func(_next_iter_num);
            return false;
        }

        template<typename jIter_t>
        int inc_type(std::vector<jIter_t>& iters, int32_t iter_num, value_type& data)
        {
            jIter_t& iter = iters[iter_num];

            const bool iter_has_more = inc_core(iter, data);
            begin = !iter_has_more;
            return iter_num + !iter_has_more;
        }

        t1_vector_t _t1_iters;
        t2_vector_t _t2_iters;
        generator_type _prefix_func;
        generator_type _suffix_func;
        end_tag_gen_type _end_func;
        size_t _next_iter_num = 0;
        bool begin = false;
        bool _end_func_run = false;
    };

} // namespace imhotep

#endif
