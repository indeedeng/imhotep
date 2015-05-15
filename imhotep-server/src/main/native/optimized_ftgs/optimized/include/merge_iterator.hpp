#ifndef MERGE_ITERATOR_HPP
#define MERGE_ITERATOR_HPP

#include <algorithm>
#include <queue>
#include <vector>

#include <boost/iterator/iterator_facade.hpp>

#include "split_iterator.hpp"

namespace imhotep {

    class merge_int_term_iterator
        : public boost::iterator_facade<merge_int_term_iterator, IntTerm const,
                                        boost::forward_traversal_tag> {

    public:
        merge_int_term_iterator() { }

        template <typename iterator>
        merge_int_term_iterator(iterator begin, iterator end)
            : _its(begin, end)
            , _queue(CompareIt(), _its) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        struct CompareIt {
            bool operator()(const split_int_term_iterator& thing1,
                            const split_int_term_iterator& thing2) {
                return *thing1 < *thing2;
            }
        };

        typedef std::priority_queue<
            split_int_term_iterator,
            std::vector<split_int_term_iterator>,
            CompareIt
            > PriorityQueue;

        void increment() {
            static split_int_term_iterator end;

            if (_queue.empty()) {
                _its.clear();
                return;
            }

            split_int_term_iterator it(_queue.top());
            _current = *it;
            _queue.pop();
            ++it;
            if (it != end) {
                _queue.push(it);
            }
        }

        bool equal(const merge_int_term_iterator& other) const {
            return _its == other._its;
        }

        const IntTerm& dereference() const {
            return _current;
        }

        std::vector<split_int_term_iterator> _its;
        PriorityQueue _queue;
        IntTerm       _current;
    };

}

#endif
