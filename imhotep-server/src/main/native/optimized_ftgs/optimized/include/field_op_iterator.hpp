#ifndef FIELD_OP_ITERATOR_HPP
#define FIELD_OP_ITERATOR_HPP

#include <sstream>

#include <boost/iterator/iterator_facade.hpp>

#include "term_providers.hpp"
#include "tgs_op_iterator.hpp"

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

        Operation<term_t>     _operation;
        TGSOpIterator<term_t> _tgs_current;
        TGSOpIterator<term_t> _tgs_end;
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
        TermSeqIterator<term_t>     ts_begin(provider.term_seq_it(_split));
        TermSeqIterator<term_t>     ts_end;
        _operation = Operation<term_t>::field_start(_split, field_name);
        _tgs_current = TGSOpIterator<term_t>(_operation, ts_begin, ts_end);
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
            if (_tgs_current != _tgs_end) {
                _operation = *_tgs_current; // tgs
            }
            else {
                _operation = Operation<term_t>::field_end(_operation);
            }
            break;
        case FIELD_END:
            ++_current;
            if (_current != _end) {
                reset_field();
            }
            else {
                _operation = Operation<term_t>();
            }
            break;
        case TGS:
            ++_tgs_current;
            if (_tgs_current != _tgs_end) {
                _operation = *_tgs_current; // tgs
            }
            else {
                _operation = Operation<term_t>::field_end(_operation);
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
        const bool tgs_equal(_tgs_current == other._tgs_current);
        return eofs && other_eofs && ops_equal && tgs_equal;
    }

} // namespace imhotep

#endif
