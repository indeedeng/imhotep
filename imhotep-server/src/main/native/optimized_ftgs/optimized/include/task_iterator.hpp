#ifndef TASK_ITERATOR_HPP
#define TASK_ITERATOR_HPP

#include <functional>

#include <boost/iterator/iterator_facade.hpp>

#include "field_op_iterator.hpp"
#include "imhotep_error.hpp"
#include "term.hpp"
#include "term_providers.hpp"

extern "C" {
    #include "local_session.h"
    #include "imhotep_native.h"
}

namespace imhotep {

    typedef std::function<void(void)> Task;

    class TaskIterator
        : public boost::iterator_facade<TaskIterator,
                                        Task const,
                                        boost::forward_traversal_tag> {
    public:
        TaskIterator(struct worker_desc&              worker,
                     struct session_desc&             session,
                     size_t                           split,
                     int                              socket_fd,
                     const TermProviders<IntTerm>&    int_providers,
                     const TermProviders<StringTerm>& str_providers)
            : _worker(worker)
            , _session(session)
            , _split(split)
            , _socket_fd(socket_fd)
            , _int_current(int_providers, split)
            , _str_current(str_providers, split) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        void increment() {
            if (_int_current == _int_end && _str_current == _str_end) return;

            if (_int_current != _int_end) {
                struct worker_desc* worker_ptr(&_worker);
                const Operation<IntTerm> op(*_int_current);
                int socket_fd(_socket_fd);
                switch (op.op_code()) {
                case FIELD_START:
                    _task = [worker_ptr, op, socket_fd]() {
                        int err = worker_start_field(worker_ptr,
                                                     op.field_name().c_str(),
                                                     op.field_name().length(),
                                                     op.field_type(),
                                                     socket_fd);
                        if (err != 0) {
                            // !@# fix error message
                            throw imhotep_error(__FUNCTION__);
                        }
                    };
                    break;
                case TGS:
                    break;
                case FIELD_END:
                    _task = [worker_ptr, op, socket_fd]() {
                        int err = worker_end_field(worker_ptr, socket_fd);
                        if (err != 0) {
                            // !@# fix error message
                            throw imhotep_error(__FUNCTION__);
                        }
                    };
                    break;
                case NO_MORE_FIELDS:
                    _task = [worker_ptr, op, socket_fd]() {
                        int err = worker_end_stream(worker_ptr, socket_fd);
                        if (err != 0) {
                            // !@# fix error message
                            throw imhotep_error(__FUNCTION__);
                        }
                    };
                    break;
                default:        // !@# 
                    break;
                }
            }
        }

        bool equal(const TaskIterator& other) const {
            // !@# revisit
            return _int_current == other._int_current &&
                _str_current == other._str_current;
        }

        const Task& dereference() const {
            return _task;
        }

        Task _empty_task = [](){ };
        Task _task = _empty_task;

        struct worker_desc&  _worker;
        struct session_desc& _session;

        const size_t _split;
        const int    _socket_fd;

        FieldOpIterator<IntTerm> _int_current;
        FieldOpIterator<IntTerm> _int_end;

        FieldOpIterator<StringTerm> _str_current;
        FieldOpIterator<StringTerm> _str_end;
    };

} // namespace imhotep

#endif
