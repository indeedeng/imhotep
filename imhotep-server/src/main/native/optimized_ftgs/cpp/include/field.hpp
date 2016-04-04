/** @field.hpp

    Inverted FlamdexField, i.e. an array of values indexed by docid.

    TODO(johnf): Consider a scheme for optimizing storage of int terms. In
    JavaLand, we use different width arrays depending on max/min term values but
    this seriously complicates and bloats the code. For now, my assumption is
    that the overall c++ savings will obviate the need for such an optimization
    and so I'll keep the damn thing simple. (One such scheme might be a sparse
    vector implementation.)
 */
#ifndef FIELD_HPP
#define FIELD_HPP


#include "docid_iterator.hpp"
#include "string_range.hpp"
#include "term.hpp"
#include "term_iterator.hpp"

#include <cstdint>
#include <vector>

namespace imhotep {

    template <typename term_t>
    struct FieldTraits {
        typedef void value_t;
    };

    template <typename term_t>
    class Field {
        typedef typename FieldTraits<term_t>::value_t value_t;

        std::vector<value_t> _values;
        value_t _min, _max;

    public:
        Field(const VarIntView& term_view,
              const VarIntView& docid_view,
              size_t n_docs=1024) {

            _values.reserve(n_docs);

            TermIterator<term_t> tit(term_view);
            TermIterator<term_t> tend;
            if (tit != tend) {
                _min = tit->id();
                _max = tit->id();
            }
            while (tit != tend ) {
                const value_t value(tit->id());
                DocIdIterator dit(docid_view, tit->doc_offset(), tit->doc_freq());
                DocIdIterator dend;
                while (dit != dend) {
                    const size_t docid(*dit);
                    if (_values.size() < docid + 1) {
                        _values.resize(docid + 1);
                    }
                    _values[docid] = value;
                    _min = std::min(_min, value);
                    _max = std::max(_max, value);
                    ++dit;
                }
                ++tit;
            }
        }

        const std::vector<value_t>& operator()() const { return _values; }

        value_t min() const { return _min; }
        value_t max() const { return _max; }
    };

    template <> struct FieldTraits<IntTerm> {
        typedef int64_t value_t;
    };

    template <> struct FieldTraits<StringTerm> {
        typedef StringRange value_t;
    };

} // namespace imhotep

#endif
