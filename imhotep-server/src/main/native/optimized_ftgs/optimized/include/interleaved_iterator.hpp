#ifndef INTERLEAVED_ITERATOR_HPP
#define INTERLEAVED_ITERATOR_HPP

#include <algorithm>
#include <iterator>
#include <memory>
#include <queue>
#include <vector>

#include <boost/iterator/iterator_facade.hpp>

namespace imhotep {

    template <typename iter_t>
    class interleaved_iterator
        : public boost::iterator_facade<interleaved_iterator<iter_t>,
                                        typename std::iterator_traits<iter_t>::value_type const,
                                        boost::forward_traversal_tag> {
    public:
        typedef typename std::iterator_traits<iter_t>::value_type value_t;

        interleaved_iterator() : _queue(0) { }

        template <typename iterator>
        interleaved_iterator(iterator begin, iterator end) {
            std::for_each(begin, end, [this] (std::pair<iter_t, iter_t> it) { _queue->push(it); });
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        void increment() {
            if (!_queue) return;

            if (_queue->empty()) {
                _queue.reset();
                return;
            }

            std::pair<iter_t, iter_t> cursor(_queue->front());
            _current = *cursor.first;
            _queue->pop();
            ++cursor.first;
            if (cursor.first != cursor.second) {
                _queue->push(cursor);
            }
        }

        bool equal(const interleaved_iterator& other) const {
            return (!_queue && !other._queue) ||
                (_queue && other._queue && (_queue == other._queue));
        }

        const value_t& dereference() const { return _current; }

        value_t _current;

        typedef std::queue<std::pair<iter_t, iter_t>> queue_t;
        std::shared_ptr<queue_t> _queue = std::make_shared<queue_t>();
    };

} // namespace imhotep

#endif
