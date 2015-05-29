#ifndef INTERLEAVED_ITERATOR_HPP
#define INTERLEAVED_ITERATOR_HPP

#include <memory>
#include <queue>

namespace imhotep {

    template <typename jIter_t>
    class InterleavedJIterator {
    public:
        typedef typename jIter_t::value_type value_type;

        InterleavedJIterator() : _queue() { }

        template<typename iterator>
        InterleavedJIterator(iterator begin, iterator end)
        {
            std::for_each(begin, end, [this] (jIter_t it) {
                if (it.hasNext()) {
                    _queue.push(it);
                }
            });
        }

        void next(value_type& data) {
            if (_queue.empty()) {
                return;
            }

            jIter_t& current_jIter = _queue.front();
            _queue.pop();
            current_jIter.next(data);
            if (current_jIter.hasNext()) {
                _queue.push(current_jIter);
            }
        }

        bool hasNext() const { return !_queue.empty(); }

    private:
        std::queue<jIter_t> _queue;
    };

} // namespace imhotep

#endif
