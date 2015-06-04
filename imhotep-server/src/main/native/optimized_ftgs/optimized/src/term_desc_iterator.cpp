#include "term_desc_iterator.hpp"

namespace imhotep {

    template <typename term_t>
    void TermDescIterator<term_t>::next(value_type& data)
    {
        if (_begin == _end) {
            data = TermDesc();
            return;
        }

        data.reset(_begin->first.id());
        const typename term_t::id_t current_id(_begin->first.id());
        while (_begin != _end && _begin->first.id() == current_id) {
            data.append((*_begin).first, (*_begin).second);
            ++_begin;
        }
    }

    template <typename term_t>
    bool TermDescIterator<term_t>::hasNext(void) const
    {
        return _begin != _end;
    }

    template void TermDescIterator<IntTerm>::next(TermDesc& data);
    template void TermDescIterator<StringTerm>::next(TermDesc& data);

    template bool TermDescIterator<IntTerm>::hasNext(void) const;
    template bool TermDescIterator<StringTerm>::hasNext(void) const;

} // namespace imhotep
