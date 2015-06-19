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
#include <future>
#include <functional>
#include <stdexcept>

#include "imhotep_error.hpp"

namespace imhotep
{

    class ExecutorService {
    public:
        /** the constructor just launches some amount of workers */
        //        ExecutorService(size_t threads=std::thread::hardware_concurrency());
        ExecutorService(size_t threads=8);

        /** the destructor joins all threads */
        ~ExecutorService();

        /** block until all workers complete or an unrecoverable error occurs */
        void await_completion(void);

        /** @return the number of worker threads in the executor's pool */
        size_t num_workers() const { return _workers.size(); }

        /** add new work item to the pool */
        template<class F, class... Args>
        auto enqueue(F&& f, Args&&... args)
            -> std::future<typename std::result_of<F(Args...)>::type>
        {
            using return_type = typename std::result_of<F(Args...)>::type;

            auto task = std::make_shared< std::packaged_task<return_type()> >(
                    std::bind(std::forward<F>(f), std::forward<Args>(args)...)
                );

            std::future<return_type> res = task->get_future();
            {
                std::unique_lock<std::mutex> lock(_mutex);

                // don't allow enqueueing after stopping the pool
                if (_stop) {
                    throw imhotep_error("enqueue on stopped ThreadPool");
                }

                _tasks.emplace([task]() {(*task)();});
            }
            _condition.notify_one();
            return res;
        }

    private:
        // need to keep track of threads so we can join them
        std::vector<std::thread> _workers;
        // the task queue
        std::queue<std::function<void()> > _tasks;

        // to keep track of the cause if a failure has shut everything down
        std::exception_ptr _failure_cause;

        // number of tasks still executing
        int _num_tasks_running;

        // synchronization
        std::mutex _mutex;
        std::condition_variable _condition;
        std::condition_variable _completion_condition;
        bool _stop = false;
    };

} // namespace imhotep

#endif
