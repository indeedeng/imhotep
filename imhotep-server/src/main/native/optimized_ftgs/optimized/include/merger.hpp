#ifndef MERGER_HPP
#define MERGER_HPP

#include <atomic>
#include <functional>
#include <limits>
#include <memory>
#include <vector>

#include <boost/lockfree/queue.hpp>

#include "term.hpp"

namespace imhotep {

    template <typename TERM_TYPE>
    class TermQueue : public boost::lockfree::queue<TERM_TYPE> {
    public:
        typedef std::shared_ptr<TermQueue> Ptr; // !@# ultimately replace with intrusive_ptr...

        static constexpr TERM_TYPE NIL = TERM_TYPE(0, 0, 0);
        static constexpr TERM_TYPE EOQ =
            TERM_TYPE(std::numeric_limits<typename TERM_TYPE::id_type>::max(),
                      std::numeric_limits<uint64_t>::max(),
                      std::numeric_limits<uint64_t>::max());
    };

    /** Notes: only one thread is allowed to insert into a given input
        queue at a time.
    */
    template <typename TERM_TYPE>
    class Merger{
    public:
        typedef TermQueue<TERM_TYPE>  Queue;
        typedef std::pair<typename Queue::Ptr, TERM_TYPE> QueueTermPair;
        typedef std::function<void(QueueTermPair&)> InsertAndProcess;

        /** I am not threadsafe. */
        InsertAndProcess attach(typename Queue::Ptr queue) {
            _inputs.push_back(std::make_pair(queue, Queue::NIL));
            ++_empty_inputs;
            return [&](QueueTermPair&) { process(); };
        }

        void join() {
            // tbd...
        }

    private:
        std::vector<QueueTermPair> _inputs;
        std::atomic_int            _empty_inputs = 0;

        void process(QueueTermPair& pair) {
            /* insert pair.second into pair.first (the queue), unless
               the queue's current value is empty, in which case we
               just set it directly, decrementing _empty_inputs. When
               _empty_inputs is zero, it's time to produce a new value
               (or values) and try to pop all our queues
            */
        }
    };

} // namespace imhotep


#endif
