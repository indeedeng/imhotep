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

    class TaskIterator
        : public boost::iterator_facade<TaskIterator,
                                        int const,
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

        template <typename term_t>
        int start_field(const Operation<term_t>& op) {
            return worker_start_field(_worker,
                                      op.field_name().c_str(),
                                      op.field_name().length(),
                                      op.field_type(),
                                      _worker_id);
        }

        template <typename term_t>
        int tgs(const Operation<term_t>& op);

        int end_field() { return worker_end_field(_worker, _worker_id); }

        int end_stream() { return worker_end_stream(_worker, _worker_id); }

        void increment();

        bool equal(const TaskIterator& other) const {
            return _int_current  == other._int_current
                && _str_current  == other._str_current
                && complete() == other.complete();
        }

        int dereference() const { return _err; }

        bool complete() const {
            return _stream_ended
                && _worker == nullptr
                && _session == nullptr;
        }

        int _err;

        struct worker_desc*  _worker  = nullptr;
        struct session_desc* _session = nullptr;

        size_t _split     = 0;
        size_t _worker_id = 0;

        FieldOpIterator<IntTerm> _int_current;
        FieldOpIterator<IntTerm> _int_end;

        FieldOpIterator<StringTerm> _str_current;
        FieldOpIterator<StringTerm> _str_end;

        bool _stream_ended = false;
    };

    template <> inline
    int TaskIterator::tgs<IntTerm>(const Operation<IntTerm>& op) {
        return run_tgs_pass(_worker, _session,
                            op.field_type(),
                            op.term_seq().id(),
                            nullptr, 0,
                            op.term_seq().docid_addresses().data(),
                            op.term_seq().doc_freqs().data(),
                            op.term_seq().tables().data(),
                            op.term_seq().size(),
                            _worker_id);
    }

    template <> inline
    int TaskIterator::tgs<StringTerm>(const Operation<StringTerm>& op) {
        return run_tgs_pass(_worker, _session,
                            op.field_type(),
                            0, // unused
                            op.term_seq().id().c_str(),
                            op.term_seq().id().length(),
                            op.term_seq().docid_addresses().data(),
                            op.term_seq().doc_freqs().data(),
                            op.term_seq().tables().data(),
                            op.term_seq().size(),
                            _worker_id);
    }

    inline
    void TaskIterator::increment() {
        if (_int_current != _int_end) {
            const Operation<IntTerm> op(*_int_current);
            switch (op.op_code()) {
            case FIELD_START:    _err = start_field(op);  break;
            case TGS:            _err = tgs<IntTerm>(op); break;
            case FIELD_END:      _err = end_field();      break;
            default:
                throw imhotep_error(__FUNCTION__ + std::string(" s/w error!"));
                break;
            }
            ++_int_current;
        }
        else if (_str_current != _str_end) {
            const Operation<StringTerm> op(*_str_current);
            switch (op.op_code()) {
            case FIELD_START:    _err = start_field(op);     break;
            case TGS:            _err = tgs<StringTerm>(op); break;
            case FIELD_END:      _err = end_field();         break;
            default:
                throw imhotep_error(__FUNCTION__ + std::string(" s/w error!"));
                break;
            }
            ++_str_current;
        }
        else if (!_stream_ended) {
            _err = end_stream();
            _stream_ended = true;
        }
        else {
            _err     = 0;
            _worker  = nullptr;
            _session = nullptr;
        }
    }

} // namespace imhotep

#endif
