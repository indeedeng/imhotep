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

        FieldOpIterator(const TermProviders<term_t>& providers, size_t split)
            : _current(providers.begin())
            , _end(providers.end())
            , _split(split) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        void increment() {
            if (_current == _end && _tgs_current == _tgs_end) return;

            switch (_operation.op_code()) {
            case INVALID:
                increment_field();
                break;
            case FIELD_START:
                _operation = *_tgs_current;
                ++_tgs_current;
                break;
            case TGS:
                if (_tgs_current != _tgs_end) {
                    _operation = *_tgs_current;
                    ++_tgs_current;
                }
                else {
                    _operation = Operation<term_t>::field_end(_operation);
                }
                break;
            case FIELD_END:
                if (_current != _end) {
                    increment_field();
                }
                else {
                    _operation = Operation<term_t>();
                }
                break;
            case NO_MORE_FIELDS:
            default:
                // !@# software error?
                break;
            }
        }

        void increment_field() {
            bool found_one(false);
            while (!found_one && _current != _end) {
                const std::string&          field_name(_current->first);
                const TermProvider<term_t>& provider(_current->second);
                TermSeqIterator<term_t> ts_begin(provider.term_seq_it(_split));
                TermSeqIterator<term_t> ts_end;
                if (ts_begin != ts_end) {
                    found_one = true;
                    _operation = Operation<term_t>::field_start(_split, field_name);
                    _tgs_current = TGSOpIterator<term_t>(_operation, ts_begin, ts_end);
                }
                else {
                    ++_current;
                }
            }
            // if (!found_one) {
            //     _operation = Operation<term_t>();
            // }
        }

        bool equal(const FieldOpIterator& other) const {
            return
                (_current == other._current ||
                 (_current == _end && other._current == other._end)) &&
                _tgs_current == other._tgs_current;
        }

        const Operation<term_t>& dereference() const { return _operation; }

        typename TermProviders<term_t>::const_iterator _current;
        typename TermProviders<term_t>::const_iterator _end;

        size_t _split = 0;

        Operation<term_t>     _operation;
        TGSOpIterator<term_t> _tgs_current;
        TGSOpIterator<term_t> _tgs_end;
    };

} // namespace imhotep

#endif
