#include "executor_service.hpp"

namespace imhotep {

    ExecutorService::ExecutorService(size_t threads)
        : _num_tasks_running(threads)  {

        for (size_t i = 0; i < threads; ++i) {
            _workers.emplace_back( [this] {
                    for(;;) {
                        std::function<void()> task;
                        {
                            std::unique_lock<std::mutex> lock(_mutex);

                            _num_tasks_running --;

                            if (_num_tasks_running == 0 && _tasks.empty()) {
                                _completion_condition.notify_all();
                            }

                            _condition.wait(lock, [this] {
                                    return _stop || !_tasks.empty();}
                                );

                            if (_stop) {
                                return;
                            }

                            task = std::move(_tasks.front());
                            _tasks.pop();

                            _num_tasks_running ++;
                        }

                        try {
                            task();
                        } catch(...) {
                            {
                                std::unique_lock<std::mutex> lock(_mutex);

                                _stop = true;
                                if (!_failure_cause) {
                                    _failure_cause = std::current_exception();
                                }
                            }

                            _completion_condition.notify_all();
                        }
                    }
                });
            }
        }

    void ExecutorService::await_completion(void) {
        std::unique_lock<std::mutex> lock(_mutex);

        _completion_condition.wait(lock, [this] {
                return (_tasks.empty() && _num_tasks_running == 0)
                    || _stop;
            } );

        if (_failure_cause)  std::rethrow_exception(_failure_cause);
    }

    ExecutorService::~ExecutorService() {
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _stop = true;
        }
        _condition.notify_all();
        for (std::thread &worker : _workers) {
            worker.join();
        }

        _completion_condition.notify_all();
    }

} // namespace imhotep
