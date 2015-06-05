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

        template <typename term_t>
        Task start_field(const Operation<term_t>& op) {
            struct worker_desc*  worker_ptr(&_worker);
            int                  socket_fd(_socket_fd);
            return [worker_ptr, op, socket_fd]() {
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
        }

        template <typename term_t> Task tgs(const Operation<term_t>& op);

        Task end_field() {
            struct worker_desc* worker_ptr(&_worker);
            int                 socket_fd(_socket_fd);
            return [worker_ptr, socket_fd]() {
                int err = worker_end_field(worker_ptr, socket_fd);
                if (err != 0) {
                    // !@# fix error message
                    throw imhotep_error(__FUNCTION__);
                }
            };
        }

        Task end_stream() {
            struct worker_desc* worker_ptr(&_worker);
            int                 socket_fd(_socket_fd);
            return [worker_ptr, socket_fd]() {
                int err = worker_end_stream(worker_ptr, socket_fd);
                if (err != 0) {
                    // !@# fix error message
                    throw imhotep_error(__FUNCTION__);
                }
            };
        }

        void increment();

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

    template <> inline
    Task TaskIterator::tgs<IntTerm>(const Operation<IntTerm>& op) {
        struct worker_desc*  worker_ptr(&_worker);
        struct session_desc* session_ptr(&_session);
        int                  socket_fd(_socket_fd);
        return [worker_ptr, session_ptr, op, socket_fd]() {
            int err = run_tgs_pass(worker_ptr, session_ptr,
                                   op.field_type(),
                                   op.term_seq().id(),
                                   nullptr, 0,
                                   op.term_seq().docid_addresses().data(),
                                   op.term_seq().doc_freqs().data(),
                                   op.term_seq().tables().data(),
                                   op.term_seq().size(),
                                   socket_fd);
            if (err != 0) {
                // !@# fix error message
                throw imhotep_error(__FUNCTION__);
            }
        };
    }

    template <> inline
    Task TaskIterator::tgs<StringTerm>(const Operation<StringTerm>& op) {
        struct worker_desc*  worker_ptr(&_worker);
        struct session_desc* session_ptr(&_session);
        int                  socket_fd(_socket_fd);
        return [worker_ptr, session_ptr, op, socket_fd]() {
            int err = run_tgs_pass(worker_ptr, session_ptr,
                                   op.field_type(),
                                   0, // unused
                                   op.term_seq().id().c_str(),
                                   op.term_seq().id().length(),
                                   op.term_seq().docid_addresses().data(),
                                   op.term_seq().doc_freqs().data(),
                                   op.term_seq().tables().data(),
                                   op.term_seq().size(),
                                   socket_fd);
            if (err != 0) {
                // !@# fix error message
                throw imhotep_error(__FUNCTION__);
            }
        };
    }

    inline
    void TaskIterator::increment() {
        if (_int_current != _int_end) {
            const Operation<IntTerm> op(*_int_current);
            switch (op.op_code()) {
            case FIELD_START:    _task = start_field(op);  break;
            case TGS:            _task = tgs<IntTerm>(op); break;
            case FIELD_END:      _task = end_field();      break;
            case NO_MORE_FIELDS: _task = end_stream();     break;
            default:        // !@#
                break;
            }
            ++_int_current;
        }
        else if (_str_current != _str_end) {
            const Operation<StringTerm> op(*_str_current);
            switch (op.op_code()) {
            case FIELD_START:    _task = start_field(op);     break;
            case TGS:            _task = tgs<StringTerm>(op); break;
            case FIELD_END:      _task = end_field();         break;
            case NO_MORE_FIELDS: _task = end_stream();        break;
            default:        // !@#
                break;
            }
            ++_str_current;
        }
    }


} // namespace imhotep

#endif
