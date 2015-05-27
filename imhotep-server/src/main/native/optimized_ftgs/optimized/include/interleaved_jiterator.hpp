#ifndef INTERLEAVED_ITERATOR_HPP
#define INTERLEAVED_ITERATOR_HPP

#include <memory>
#include <queue>

namespace imhotep {

    template <typename jIter_t>
    class InterleavedJIterator {
    public:
        typedef typename jIter_t::value_type value_type;

        InterleavedJIterator() : _queue(0) { }

        template<typename iterator>
        InterleavedJIterator(iterator begin, iterator end)
        {
            std::for_each(begin, end, [this] (jIter_t it) {
                if (it.hasNext()) {
                    _queue->push(it);
                }
            });
        }

    private:
        friend class boost::iterator_core_access;

        void next(value_type& data) {
            if (!_queue) return;

            if (_queue->empty()) {
                _queue.reset();
                return;
            }

            jIter_t& current_jIter = _queue->front();
            _queue->pop();
            current_jIter.next(data);
            if (current_jIter.hasNext()) {
                _queue->push(current_jIter);
            }
        }

        bool hasNext() const { return !_queue.empty(); }

        typedef std::queue<jIter_t> queue_t;
        std::shared_ptr<queue_t> _queue = std::make_shared<queue_t>();
    };

} // namespace imhotep

#endif
