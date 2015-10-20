#include "field_op_iterator.hpp"

#include "imhotep_error.hpp"

namespace imhotep {

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
        _merge_current = provider.merge_it(_split);
        _operation.field_start(_split, field_name, _merge_current);
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
            if (_merge_current != _merge_end) {
                _operation.tgs(_merge_current);
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
            if (_merge_current != _merge_end) {
                _operation.tgs(_merge_current);
            }
            else {
                _operation.field_end(_operation);
            }
            break;
        default:
            throw imhotep_error(std::string(__PRETTY_FUNCTION__) + " invalid state");
            break;
        }
    }

    template <typename term_t>
    bool FieldOpIterator<term_t>::equal(const FieldOpIterator& other) const {
        const bool eofs(_current == _end);
        const bool other_eofs(other._current == other._end);
        const bool ops_equal(_operation == other._operation);
        const bool merge_equal(_merge_current == other._merge_current);
        return eofs && other_eofs && ops_equal && merge_equal;
    }


    /* template instantiations */
    template FieldOpIterator<IntTerm>::FieldOpIterator(const TermProviders<IntTerm>& providers, size_t split);
    template void FieldOpIterator<IntTerm>::reset_field();
    template void FieldOpIterator<IntTerm>::increment();
    template bool FieldOpIterator<IntTerm>::equal(const FieldOpIterator& other) const;

    template FieldOpIterator<StringTerm>::FieldOpIterator(const TermProviders<StringTerm>& providers, size_t split);
    template void FieldOpIterator<StringTerm>::reset_field();
    template void FieldOpIterator<StringTerm>::increment();
    template bool FieldOpIterator<StringTerm>::equal(const FieldOpIterator& other) const;


} // namespace imhotep
