#include "term_seq.hpp"

namespace imhotep {

    template <typename term_t>
    void TermSeq<term_t>::reset(MergeIterator<term_t>& begin,
                                MergeIterator<term_t>& end) {
        _id = begin != end ? (*begin)._term.id() :
            IdTraits<typename term_t::id_t>::default_value();

        _guts.clear();

        while (begin != end) {
            const MergeOutput<term_t>& merge_result(*begin);
            const term_t&              term(merge_result._term);
            Shard::packed_table_ptr    table(merge_result._table);
            const char*                docid_base(merge_result._docid_base);
            docid_addresses().push_back(docid_base + term.doc_offset());
            doc_freqs().push_back(term.doc_freq());
            tables().push_back(table);
            ++begin;
        }
    }


    /* template instantiations */
    template
    void TermSeq<IntTerm>::reset(MergeIterator<IntTerm>& begin,
                                 MergeIterator<IntTerm>& end);

    template
    void TermSeq<StringTerm>::reset(MergeIterator<StringTerm>& begin,
                                    MergeIterator<StringTerm>& end);

} // namespace imhotep
