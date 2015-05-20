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

namespace imhotep
{

    class ExecutorService {
    public:
        // the constructor just launches some amount of workers
        ExecutorService(size_t threads)
            : num_tasks_running(threads)
        {
            for (size_t i = 0; i < threads; ++i) {
                workers.emplace_back(
                 [this] {
                    for(;;) {
                        std::function<void()> task;
                        {
                            std::unique_lock<std::mutex> lock(this->_mutex);

                            this->num_tasks_running --;

                            if (this->num_tasks_running == 0 && this->tasks.empty()) {
                                this->completion_condition.notify_all();
                            }

                            this->condition.wait(lock,
                                    [this] {return this->stop || !this->tasks.empty();}
                            );

                            if (this->stop) {
                                return;
                            }

                            task = std::move(this->tasks.front());
                            this->tasks.pop();

                            this->num_tasks_running ++;
                        }

                        try {
                            task();
                        } catch(...) {
                            {
                                std::unique_lock<std::mutex> lock(this->_mutex);

                                this->stop = true;
                                if (!this->failure_cause) {
                                    this->failure_cause = std::current_exception();
                                }
                            }

                            this->completion_condition.notify_all();
                        }
                    }
                });
            }
        }

        void awaitCompletion(void)
        {
            std::unique_lock<std::mutex> lock(this->_mutex);

            completion_condition.wait(lock, [this] {
                    return (this->tasks.empty() && this->num_tasks_running == 0)
                        || this->stop;
                } );

            if (this->failure_cause)  std::rethrow_exception(this->failure_cause);
        }

        // add new work item to the pool
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
                if (stop) {
                    throw std::runtime_error("enqueue on stopped ThreadPool");
                }

                tasks.emplace([task]() {(*task)();});
            }
            condition.notify_one();
            return res;
        }

        // the destructor joins all threads
        ~ExecutorService()
        {
            {
                std::unique_lock<std::mutex> lock(_mutex);
                stop = true;
            }
            condition.notify_all();
            for (std::thread &worker : workers) {
                worker.join();
            }

            completion_condition.notify_all();
        }

    private:
        // need to keep track of threads so we can join them
        std::vector<std::thread> workers;
        // the task queue
        std::queue<std::function<void()> > tasks;

        // to keep track of the cause if a failure has shut everything down
        std::exception_ptr failure_cause;

        // number of tasks still executing
        int num_tasks_running;

        // synchronization
        std::mutex _mutex;
        std::condition_variable condition;
        std::condition_variable completion_condition;
        bool stop = false;
    };

} // namespace imhotep

#endif
