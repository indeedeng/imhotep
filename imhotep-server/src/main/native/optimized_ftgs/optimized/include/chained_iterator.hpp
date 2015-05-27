#ifndef CHAINED_ITERATOR_HPP
#define CHAINED_ITERATOR_HPP

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

        ChainedIterator(t1_vector_t&& t1_vec,
                        t2_vector_t&& t2_vec,
                        generator_type f1,
                        generator_type f2) :
                _t1_iters(t1_vec),
                _t2_iters(t2_vec),
                _prefix_func(f1),
                _suffix_func(f2),
                begin(true)
        { }


        void next(value_type& data)
        {
            if (_next_iter_num < _t1_iters.size()) {
                _next_iter_num = inc_type(_t1_iters, _next_iter_num, data);
            } else if (_next_iter_num < (_t1_iters.size() + _t2_iters.size())) {
                _next_iter_num = inc_type(_t2_iters, _next_iter_num - _t1_iters.size(), data)
                        + _t1_iters.size();
            }
        }

        bool hasNext() const
        {
            return _next_iter_num < (_t1_iters.size() + _t2_iters.size());
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

            if (end) {
                data = _suffix_func(_next_iter_num);
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
        int32_t _next_iter_num = 0;
        bool begin = false;
        bool end = false;
    };

} // namespace imhotep

#endif
