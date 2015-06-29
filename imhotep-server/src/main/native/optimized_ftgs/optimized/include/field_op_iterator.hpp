#ifndef FIELD_OP_ITERATOR_HPP
#define FIELD_OP_ITERATOR_HPP

#include <sstream>

#include <boost/iterator/iterator_facade.hpp>

#include "operation.hpp"
#include "term_providers.hpp"

namespace imhotep {

    template <typename term_t>
    class FieldOpIterator
        : public boost::iterator_facade<FieldOpIterator<term_t>,
                                        Operation<term_t> const,
                                        boost::forward_traversal_tag> {
    public:
        FieldOpIterator() { }

        FieldOpIterator(const TermProviders<term_t>& providers, size_t split);

    private:
        friend class boost::iterator_core_access;

        void reset_field();

        void increment();

        bool equal(const FieldOpIterator& other) const;

        const Operation<term_t>& dereference() const { return _operation; }

        typename TermProviders<term_t>::const_iterator _current;
        typename TermProviders<term_t>::const_iterator _end;

        size_t _split = 0;

        Operation<term_t>       _operation;
        TermSeqIterator<term_t> _ts_current;
        TermSeqIterator<term_t> _ts_end;
    };


    template <typename term_t>
    FieldOpIterator<term_t>::FieldOpIterator(const TermProviders<term_t>& providers, size_t split)
        : _current(providers.begin())
        , _end(providers.end())
        , _split(split) {
        increment();
    }

    template <typename term_t>
    void FieldOpIterator<term_t>::reset_field() {
        const std::string&          field_name(_current->first);
        const TermProvider<term_t>& provider(_current->second);
        _ts_current = TermSeqIterator<term_t>(provider.term_seq_it(_split));
        _operation.field_start(_split, field_name);
    }

    template <typename term_t>
    void FieldOpIterator<term_t>::increment() {
        switch (_operation.op_code()) {
        case INVALID:
            if (_current != _end) {
                reset_field();
            }
            break;
        case FIELD_START:
            if (_ts_current != _ts_end) {
                _operation.tgs(*_ts_current);
            }
            else {
                _operation.field_end(_operation);
            }
            break;
        case FIELD_END:
            ++_current;
            if (_current != _end) {
                reset_field();
            }
            else {
                _operation.clear();
            }
            break;
        case TGS:
            ++_ts_current;
            if (_ts_current != _ts_end) {
                _operation.tgs(*_ts_current);
            }
            else {
                _operation.field_end(_operation);
            }
            break;
        default:
            // s/w error
            Log::error("WTF!?");
            break;
        }
    }

    template <typename term_t>
    bool FieldOpIterator<term_t>::equal(const FieldOpIterator& other) const {
        const bool eofs(_current == _end);
        const bool other_eofs(other._current == other._end);
        const bool ops_equal(_operation == other._operation);
        const bool ts_equal(_ts_current == other._ts_current);
        return eofs && other_eofs && ops_equal && ts_equal;
    }

} // namespace imhotep

#endif
