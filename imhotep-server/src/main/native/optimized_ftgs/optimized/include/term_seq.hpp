#ifndef TERM_SEQ_HPP
#define TERM_SEQ_HPP

#include <vector>

#include "merge_iterator.hpp"
#include "shard.hpp"

namespace imhotep {

    template <typename term_t>
    class TermSeq {
    public:
        typedef MergeIterator<term_t> merge_it;

        TermSeq() { }

        TermSeq(const TermSeq& rhs) = default;

        /* ids must match all items in iterator range! */
        TermSeq(merge_it begin, merge_it end)
            : _id(begin != end ?
                  begin->first.id() :
                  IdTraits<typename term_t::id_t>::default_value()) {

            while (begin != end) {
                const term_t&           term(begin->first);
                Shard::packed_table_ptr table(begin->second);
                _docid_addresses.push_back(term.doc_offset()); // !@# fix name mismatch
                _doc_freqs.push_back(term.doc_freq());
                _tables.push_back(table);
                ++begin;
            }
        }

        bool operator==(const TermSeq& rhs) const {
            return
                _id              == rhs._id              &&
                _docid_addresses == rhs._docid_addresses &&
                _doc_freqs       == rhs._doc_freqs       &&
                _tables          == rhs._tables;
        }

        const typename term_t::id_t& id() const { return _id; }

        std::vector<int64_t> docid_addresses() const { return _docid_addresses; }
        std::vector<int32_t>       doc_freqs() const { return _doc_freqs;       }

        std::vector<Shard::packed_table_ptr> tables() const { return _tables; }

    private:
        typename term_t::id_t _id = IdTraits<typename term_t::id_t>::default_value();

        std::vector<int64_t> _docid_addresses;
        std::vector<int32_t> _doc_freqs;

        std::vector<Shard::packed_table_ptr> _tables;
    };

} // namespace imhotep

#endif
