#ifndef MERGER_HPP
#define MERGER_HPP

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include <boost/lockfree/queue.hpp>

#include "term.hpp"

namespace imhotep {

    template <typename TERM_TYPE>
    class TermQueue {
    public:
        typedef std::shared_ptr<TermQueue<TERM_TYPE>> Ptr;

        const TERM_TYPE& front() const { return _front; }

        bool empty() { return _front.empty() && _input_queue.empty(); }

        bool push(const TERM_TYPE value) {
            return _input_queue.push(value);
        }

        void refresh() {
            if (_front.empty()) _input_queue.pop(_front);
        }

        typedef std::function<void(const TERM_TYPE& term)> Consume;
        void pop(Consume& consume) {
            consume(_front);
            _front = TERM_TYPE();
        }
    private:
        typedef boost::lockfree::queue<TERM_TYPE, boost::lockfree::capacity<4096 * 2>> InputQueue;

        InputQueue _input_queue;
        TERM_TYPE  _front = TERM_TYPE();
    };

    /** Notes: only one thread is allowed to insert into a given input
        queue at a time.
    */
    template <typename TERM_TYPE>
    class Merger{
    public:
        typedef std::function<void(const TERM_TYPE& term)> InsertAndProcess;
        typedef std::function<void(const TERM_TYPE& term)> Consume;

        Merger()              = default;
        Merger(const Merger&) = delete;

        /** I am not threadsafe. */
        InsertAndProcess attach(Consume& consume) {
            term_queue_ptr term_queue(std::make_shared<TermQueue<TERM_TYPE>>());
            _queues.push_back(term_queue);
            // !@# Should consume be captured by ref?
            return [this, term_queue, &consume](const TERM_TYPE& term) {
                process(term_queue, term, consume);
            };
        }

        /** Should only be called after all producer threads have been join()-ed */
        void join(Consume& consume) {
            while (!_queues.empty()) {
                TERM_TYPE                   lowest;
                typename queues_t::iterator it(_queues.begin());

                while (it != _queues.end()) {
                    (*it)->refresh();
                    if ((*it)->empty()) {
                        it = _queues.erase(it);
                    }
                    else {
                        if (lowest.empty() || (*it)->front() < lowest) {
                            lowest = (*it)->front();
                        }
                        ++it;
                    }
                }

                if (!lowest.empty()) {
                    for (it = _queues.begin(); it != _queues.end(); ++it) {
                        if ((*it)->front() == lowest) {
                            (*it)->pop(consume);
                        }
                    }
                }
            }
        }

    private:
        typedef typename TermQueue<TERM_TYPE>::Ptr term_queue_ptr;
        typedef std::vector<term_queue_ptr>        queues_t;

        std::mutex _mutex;
        queues_t   _queues;

        void process(term_queue_ptr term_queue,
                     TERM_TYPE value,
                     Consume& consume) {
            bool pushed;
            do {
                pushed = term_queue->push(value);
                if (_mutex.try_lock()) {
                    TERM_TYPE lowest;
                    typename queues_t::iterator it(_queues.begin());
                    do {
                        (*it)->refresh();
                        if (lowest.empty() || (*it)->front() < lowest) {
                            lowest = (*it)->front();
                        }
                    } while (!lowest.empty() && ++it != _queues.end());
                    if (!lowest.empty()) {
                        for (it = _queues.begin(); it != _queues.end(); ++it) {
                            if ((*it)->front() == lowest) {
                                (*it)->pop(consume);
                            }
                        }
                    }
                    _mutex.unlock();
                }
            } while (!pushed);
        }
    };

} // namespace imhotep


#endif
