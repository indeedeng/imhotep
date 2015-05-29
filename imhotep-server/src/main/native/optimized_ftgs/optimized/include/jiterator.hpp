//
//  jIterator.h
//  imhotep
//
//  Created by darren on 5/24/15.
//  Copyright (c) 2015 indeed.com. All rights reserved.
//

#ifndef JITERATOR_H
#define JITERATOR_H

#include <functional>
#include <iterator>
#include <type_traits>

namespace imhotep {

    template<typename v_type>
    class base_jIterator {
    public:
        typedef v_type value_type;

        base_jIterator() { }

        const bool hasNext() const { return false; }

        void next(value_type& data) { }
    };


    template<typename iter_t>
    class iterator_2_jIterator {
    public:
        typedef typename std::iterator_traits<iter_t>::value_type value_type;

        iterator_2_jIterator(iter_t base_iter, iter_t end_iter) :
            _base_iterator(base_iter),
            _end_iterator(end_iter)
        { }

        const bool hasNext()
        const {
            return _base_iterator != _end_iterator;
        }

        void next(value_type& data)
        {
            data = *_base_iterator;

            _base_iterator++;
        }

    private:
        iter_t _base_iterator;
        iter_t _end_iterator;
    };


    template<typename j_iter_t, typename outer_type, class func_t>
    class wrapping_jIterator {
    public:
        typedef typename j_iter_t::value_type src_type;
        typedef outer_type value_type;

        wrapping_jIterator(void) { }

        wrapping_jIterator(j_iter_t iter, func_t func) :
            _base_jIterator(iter),
            _unwrapping_func(func)
        { }

        const bool hasNext()
        const {
            return _base_jIterator.hasNext();
        }

        void next(value_type& data)
        {
            // get _base_iter data type from the value_type data struct
            src_type& temp = _unwrapping_func(data);

            // fill with data
            _base_jIterator.next(temp);
        }

    private:
        j_iter_t _base_jIterator;
        func_t _unwrapping_func;
    };
}

#endif
