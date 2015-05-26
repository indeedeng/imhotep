//
//  jIterator.h
//  imhotep
//
//  Created by darren on 5/24/15.
//  Copyright (c) 2015 indeed.com. All rights reserved.
//

#ifndef imhotep_jIterator_h
#define imhotep_jIterator_h

#include <iterator>
#include <type_traits>

namespace imhotep {
    
    template<typename iter_t>
    class iterator_2_jIterator {
    public:
        typedef typename std::iterator_traits<iter_t>::value_type value_type;

        iterator_2_jIterator(iter_t base_iter, iter_t end_iter) :
            _base_iterator(base_iter),
            _end_iterator(end_iter)
        { }
        
        const bool has_next()
        const {
            return _base_iterator != _end_iterator;
        }
        
        void next(value_type data)
        {
            data = *_base_iterator;
            
            _base_iterator++;
        }
        
    private:
        iter_t _base_iterator;
        iter_t _end_iterator;
    };
    
    template<typename j_iter_t, class func_t>
    class transform_jIterator {
    public:
        typedef typename j_iter_t::value_type value_type;
        
        transform_jIterator(j_iter_t iter, func_t func) :
            _base_jIterator(iter),
            _transform_func(func)
        { }

        const bool has_next()
        const {
            return _base_jIterator.has_next();
        }
        
        void next(value_type data)
        {
            _transform_func(data, _base_jIterator);
        }
        
    private:
        j_iter_t _base_jIterator;
        func_t _transform_func;
    };
}

#endif
