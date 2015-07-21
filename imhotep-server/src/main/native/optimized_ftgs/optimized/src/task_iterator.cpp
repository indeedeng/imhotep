#include "task_iterator.hpp"

namespace imhotep {

    TaskIterator::TaskIterator()
        : _err(0)
        , _worker(0)
        , _session(0)
        , _split(0)
        , _stream_ended(true)
    { }

    TaskIterator::TaskIterator(struct worker_desc*              worker,
                               struct session_desc*             session,
                               size_t                           split,
                               const TermProviders<IntTerm>&    int_providers,
                               const TermProviders<StringTerm>& str_providers)
        : _err(0)
        , _worker(worker)
        , _session(session)
        , _split(split)
        , _int_current(int_providers, split)
        , _str_current(str_providers, split)
        , _stream_ended(false) {
        increment();
    }

    bool TaskIterator::equal(const TaskIterator& other) const {
        return _int_current  == other._int_current
            && _str_current  == other._str_current
            && complete() == other.complete();
    }

    bool TaskIterator::complete() const {
        return _stream_ended && _worker == 0 && _session == 0;
    }

    template <>
    int TaskIterator::tgs<IntTerm>(const Operation<IntTerm>& op) {
        return run_tgs_pass(_worker, _session,
                            op.field_type(),
                            op.term_seq().id(),
                            0, 0,
                            op.term_seq().docid_addresses().data(),
                            op.term_seq().doc_freqs().data(),
                            op.term_seq().tables().data(),
                            op.term_seq().size(),
                            _split);
    }

    template <>
    int TaskIterator::tgs<StringTerm>(const Operation<StringTerm>& op) {
        const StringRange& id(op.term_seq().id());
        return run_tgs_pass(_worker, _session,
                            op.field_type(),
                            0, // unused
                            id.c_str(), id.size(),
                            op.term_seq().docid_addresses().data(),
                            op.term_seq().doc_freqs().data(),
                            op.term_seq().tables().data(),
                            op.term_seq().size(),
                            _split);
    }

    void TaskIterator::increment() {
        if (_int_current != _int_end) {
            const Operation<IntTerm>& op(*_int_current);
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
            const Operation<StringTerm>& op(*_str_current);
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
            _worker  = 0;
            _session = 0;
        }
    }

    /* template instantiations */
    template int TaskIterator::tgs<IntTerm>(const Operation<IntTerm>& op);
    template int TaskIterator::tgs<StringTerm>(const Operation<StringTerm>& op);

} // namespace imhotep
