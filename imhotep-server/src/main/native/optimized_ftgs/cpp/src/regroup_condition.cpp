#include "regroup_condition.hpp"

/* Notes:

   Implementations of InequalityConditions are *far* from optimal,
   however it's difficult to make them better without significantly
   compromising the structure and readability of this code. Before
   addressing any performance issues with them, best to see just how
   bad they are and how often these conditions even come up in
   practice. For example:

     - We invert terms redundantly since multiple inequality
       conditions can overlap.
     - We also store inverted terms redundantly.

   In both cases, removing the redundancies requires the conditions
   having knowledge of each other, which would require some clumsy
   code.

   Also, the storage overhead required is not well accounted for up in
   JavaLand.
 */

namespace imhotep {

    template<>
    void EqualityCondition<int64_t>::reset(Shard& shard)
    {
        const IntTermIndex    term_idx(shard.int_term_index(field()));
        TermIterator<IntTerm> term_it(term_idx.find_it(term()));
        TermIterator<IntTerm> term_end;
        if (term_it != term_end) {
            const IntTerm& found(*term_it);
            VarIntView     docid_view(shard.docid_view<IntTerm>(field()));
            _doc_it = DocIdIterator(docid_view, found.doc_offset(), found.doc_freq());
        }
    }

    template<>
    void EqualityCondition<std::string>::reset(Shard& shard)
    {
        const StringTermIndex    term_idx(shard.str_term_index(field()));
        const std::string        term_str(term().begin(), term().end());
        TermIterator<StringTerm> term_it(term_idx.find_it(term_str));
        TermIterator<StringTerm> term_end;
        if (term_it != term_end) {
            const StringTerm& found(*term_it);
            VarIntView        docid_view(shard.docid_view<StringTerm>(field()));
            _doc_it = DocIdIterator(docid_view, found.doc_offset(), found.doc_freq());
        }
    }

    template<>
    void InequalityCondition<int64_t>::reset(Shard& shard)
    {
        const VarIntView      term_view(shard.term_view<IntTerm>(field()));
        const IntTermIndex    term_idx(shard.int_term_index(field()));
        TermIterator<IntTerm> term_it(term_view);
        TermIterator<IntTerm> term_end(term_idx.find_it(term()));
        const VarIntView      docid_view(shard.docid_view<IntTerm>(field()));
        while (term_it != term_end) {
            const IntTerm& term(*term_it);
            DocIdIterator  doc_it(docid_view, term.doc_offset(), term.doc_freq());
            DocIdIterator  doc_end;
            while (doc_it != doc_end) {
                _doc_ids.set(*doc_it);
                ++doc_it;
            }
            ++term_it;
        }
    }

    template<>
    void InequalityCondition<std::string>::reset(Shard& shard)
    {
        const VarIntView         term_view(shard.term_view<StringTerm>(field()));
        const StringTermIndex    term_idx(shard.str_term_index(field()));
        TermIterator<StringTerm> term_it(term_view);
        TermIterator<StringTerm> term_end(term_idx.find_it(term()));
        const VarIntView         docid_view(shard.docid_view<StringTerm>(field()));
        while (term_it != term_end) {
            const StringTerm& term(*term_it);
            DocIdIterator     doc_it(docid_view, term.doc_offset(), term.doc_freq());
            DocIdIterator     doc_end;
            while (doc_it != doc_end) {
                const docid_t docid(*doc_it);
                if (docid >= _doc_ids.size()) { // !@# naive resize operation
                    _doc_ids.resize(docid + 1);
                }
                _doc_ids.set(docid);
                ++doc_it;
            }
            ++term_it;
        }
    }

} // namespace imhotep
