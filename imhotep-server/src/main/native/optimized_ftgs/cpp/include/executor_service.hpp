/*
 * ExecutorService.h
 *
 *  Created on: May 12, 2015
 *      Author: darren
 */
#ifndef EXECUTOR_SERVICE_HPP
#define EXECUTOR_SERVICE_HPP

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <stdexcept>

#include "imhotep_error.hpp"

namespace imhotep
{

    class ExecutorService {
    public:
        /** the constructor launches one worker per processor */
        ExecutorService(size_t threads=num_processors());

        /** the destructor joins all threads */
        ~ExecutorService();

        /** block until all workers complete or an unrecoverable error occurs */
        void await_completion(void);

        /** @return the number of worker threads in the executor's pool */
        size_t num_workers() const { return _workers.size(); }

        /** add new work item to the pool */
        void enqueue(const std::function<void()>& task)
        {
            std::unique_lock<std::mutex> lock(_mutex);

            // don't allow enqueueing after stopping the pool
            if (_stop) {
                throw imhotep_error("enqueue on stopped ThreadPool");
            }
            _tasks.push(task);
            _condition.notify_one();
        }

        bool unblocked() const { return _stop || !_tasks.empty();                             }
        bool  complete() const { return (_tasks.empty() && _num_tasks_running == 0) || _stop; }

    private:
        static size_t num_processors();

        struct Unblocked {
            const ExecutorService& _self;
            Unblocked(const ExecutorService& self) : _self(self)  { }
            bool operator()() const { return _self.unblocked(); }
        };

        struct Complete {
            const ExecutorService& _self;
            Complete(const ExecutorService& self) : _self(self)  { }
            bool operator()() const { return _self.complete(); }
        };

        void work();

        // need to keep track of threads so we can join them
        std::vector<std::thread> _workers;

        // the task queue
        std::queue<std::function<void()>> _tasks;

        // to keep track of the cause if a failure has shut everything down
        std::string _failure_cause;

        // number of tasks still executing
        int _num_tasks_running;

        // synchronization
        std::mutex _mutex;
        std::condition_variable _condition;
        std::condition_variable _completion_condition;
        bool _stop;
    };

} // namespace imhotep

#endif
