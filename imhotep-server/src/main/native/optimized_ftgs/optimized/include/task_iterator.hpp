#ifndef TASK_ITERATOR_HPP
#define TASK_ITERATOR_HPP

#include <functional>
#include <sstream>

#include <boost/iterator/iterator_facade.hpp>

#include "field_op_iterator.hpp"
#include "imhotep_error.hpp"
#include "log.hpp"
#include "term.hpp"
#include "term_providers.hpp"

extern "C" {
#include "local_session.h"
#include "imhotep_native.h"
}

namespace imhotep {

    typedef std::function<void(void)> Fun;

    class Task {
    public:
        Task(const std::string& name, const Fun& fun)
            : _name(name)
            , _fun(fun)
        { }

        void operator()() const { _fun(); }

        bool operator==(const Task& rhs) const {
            return
                _name == rhs._name &&
                _fun.target<void(void)>() == rhs._fun.target<void(void)>();
        }

        const std::string& to_string() const { return _name; }

    private:
        std::string _name;
        Fun         _fun;
    };

    class TaskIterator
        : public boost::iterator_facade<TaskIterator,
                                        Task const,
                                        boost::forward_traversal_tag> {
    public:
        TaskIterator() : _stream_ended(true) { }

        TaskIterator(struct worker_desc*              worker,
                     struct session_desc*             session,
                     size_t                           split,
                     size_t                           worker_id,
                     const TermProviders<IntTerm>&    int_providers,
                     const TermProviders<StringTerm>& str_providers)
            : _worker(worker)
            , _session(session)
            , _split(split)
            , _worker_id(worker_id)
            , _int_current(int_providers, split)
            , _str_current(str_providers, split) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        static std::string error_from(struct worker_desc* worker) {
            return std::string(worker->error.str);
        }

        template <typename term_t>
        Task start_field(const Operation<term_t>& op) {
            // Log::debug(__FUNCTION__ + std::string(" ") + op.to_string());
            struct worker_desc* worker(_worker);
            const int           worker_id(_worker_id);
            return Task(__FUNCTION__,
                        [op, worker, worker_id]() {
                            int err = worker_start_field(worker,
                                                         op.field_name().c_str(),
                                                         op.field_name().length(),
                                                         op.field_type(),
                                                         worker_id);
                            if (err != 0) throw imhotep_error(TaskIterator::error_from(worker));
                        });
        }

        template <typename term_t> Task tgs(const Operation<term_t>& op);

        Task end_field() {
            // Log::debug(__FUNCTION__);
            struct worker_desc* worker(_worker);
            const int           worker_id(_worker_id);
            return Task(__FUNCTION__,
                        [worker, worker_id]() {
                            int err = worker_end_field(worker, worker_id);
                            if (err != 0) throw imhotep_error(TaskIterator::error_from(worker));
                        });
        }

        Task end_stream() {
            // Log::debug(__FUNCTION__);
            struct worker_desc* worker(_worker);
            const int           worker_id(_worker_id);
            return Task(__FUNCTION__,
                        [worker, worker_id]() {
                            // Log::debug(__FUNCTION__);
                            int err = worker_end_stream(worker, worker_id);
                            if (err != 0) throw imhotep_error(TaskIterator::error_from(worker));
                        });
        }

        void increment();

        bool equal(const TaskIterator& other) const {
            const bool int_current(_int_current  == other._int_current);
            const bool str_current(_str_current  == other._str_current);
            const bool stream_ended(_stream_ended == other._stream_ended);
            const bool task(_task == other._task );

            // std::ostringstream os;
            // os << __FUNCTION__
            //    << " int_current: " << int_current
            //    << " str_current: " << str_current
            //    << " stream_ended: " << stream_ended
            //    << " task: " << task;
            // Log::debug(os.str());

            return int_current && str_current && stream_ended && task;
        }

        const Task& dereference() const {
            return _task;
        }

        Task _empty_task = Task(std::string("<empty>"), Fun([](void){}));
        Task _task = _empty_task;

        struct worker_desc*  _worker  = nullptr;
        struct session_desc* _session = nullptr;

        size_t _split     = 0;
        size_t _worker_id = 0;

        FieldOpIterator<IntTerm> _int_current;
        FieldOpIterator<IntTerm> _int_end;

        FieldOpIterator<StringTerm> _str_current;
        FieldOpIterator<StringTerm> _str_end;

        bool _stream_ended = false;

        std::vector<long> to_longs(const std::vector<const char*>& in_addrs) {
            std::vector<long> result;
            for (auto addr: in_addrs) {
                result.push_back(reinterpret_cast<long>(addr));
            }
            return result;
        }
    };

    template <> inline
    Task TaskIterator::tgs<IntTerm>(const Operation<IntTerm>& op) {
        // Log::debug(__FUNCTION__ + std::string(" ") + op.to_string());
        struct worker_desc*  worker(_worker);
        struct session_desc* session(_session);
        const int            worker_id(_worker_id);
        return Task(__FUNCTION__,
                    [op, worker, session, worker_id]() {
                        int err = run_tgs_pass(worker, session,
                                               op.field_type(),
                                               op.term_seq().id(),
                                               nullptr, 0,
                                               op.term_seq().docid_addresses().data(),
                                               op.term_seq().doc_freqs().data(),
                                               op.term_seq().tables().data(),
                                               op.term_seq().size(),
                                               worker_id);
                        if (err != 0) throw imhotep_error(TaskIterator::error_from(worker));
                    });
    }

    template <> inline
    Task TaskIterator::tgs<StringTerm>(const Operation<StringTerm>& op) {
        // Log::debug(__FUNCTION__ + std::string(" ") + op.to_string());
        struct worker_desc*  worker(_worker);
        struct session_desc* session(_session);
        const int            worker_id(_worker_id);
        return Task(__FUNCTION__,
                    [op, worker, session, worker_id]() {
                        int err = run_tgs_pass(worker, session,
                                   op.field_type(),
                                   0, // unused
                                   op.term_seq().id().c_str(),
                                   op.term_seq().id().length(),
                                   op.term_seq().docid_addresses().data(),
                                   op.term_seq().doc_freqs().data(),
                                   op.term_seq().tables().data(),
                                   op.term_seq().size(),
                                   worker_id);
                        if (err != 0) throw imhotep_error(TaskIterator::error_from(worker));
                    });
    }

    inline
    void TaskIterator::increment() {
        if (_int_current != _int_end) {
            const Operation<IntTerm> op(*_int_current);
            switch (op.op_code()) {
            case FIELD_START:    _task = start_field(op);  break;
            case TGS:            _task = tgs<IntTerm>(op); break;
            case FIELD_END:      _task = end_field();      break;
            default:
                Log::debug(__FUNCTION__ + std::string("!@# WTF?"));
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
            default:
                Log::debug(__FUNCTION__ + std::string("!@# WTF?"));
                break;
            }
            ++_str_current;
        }
        else if (!_stream_ended) {
            // Log::debug(__FUNCTION__ + std::string(" ending steam"));
            _task = end_stream();
            _stream_ended = true;
        }
        else {
            _task = _empty_task;
        }
    }

} // namespace imhotep

#endif
