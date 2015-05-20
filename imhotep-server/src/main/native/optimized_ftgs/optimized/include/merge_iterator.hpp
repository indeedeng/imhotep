#ifndef MERGE_ITERATOR_HPP
#define MERGE_ITERATOR_HPP

#include <algorithm>
#include <queue>
#include <vector>

#include <boost/iterator/iterator_facade.hpp>

#include "split_iterator.hpp"

namespace imhotep {

    template <typename term_t>
    class MergeIterator
        : public boost::iterator_facade<MergeIterator<term_t>,
                                        term_t const,
                                        boost::forward_traversal_tag> {
    public:
        MergeIterator() { }

        template <typename iterator>
        MergeIterator(iterator begin, iterator end)
            : _its(begin, end)
            , _queue(CompareIt(), _its) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        struct CompareIt {
            bool operator()(const SplitIterator<term_t>& thing1,
                            const SplitIterator<term_t>& thing2) {
                return *thing1 > *thing2;
            }
        };

        typedef std::priority_queue<
            SplitIterator<term_t>,
            std::vector<SplitIterator<term_t>>,
            CompareIt
            > PriorityQueue;

        void increment() {
            static SplitIterator<term_t> end;

            if (_queue.empty()) {
                _its.clear();
                return;
            }

            SplitIterator<term_t> it(_queue.top());
            _current = *it;
            _queue.pop();
            ++it;
            if (it != end) {
                _queue.push(it);
            }
        }

        bool equal(const MergeIterator& other) const {
            return _its == other._its;
        }

        const term_t& dereference() const {
            return _current;
        }

        std::vector<SplitIterator<term_t>> _its;

        PriorityQueue _queue;
        term_t        _current;
    };
}

#endif
