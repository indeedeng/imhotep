#ifndef TERM_DESC_ITERATOR_HPP
#define TERM_DESC_ITERATOR_HPP

#include <cassert>
#include <vector>
#include "merge_iterator.hpp"
#include "shard.hpp"

namespace imhotep {

    class TermDesc {
    public:
        int64_t               int_term() const { return _int_term;    }
        const std::string& string_term() const { return _string_term; }

        const int64_t* docid_addresses() const { return _docid_addresses.data(); }
        const int64_t*       doc_freqs() const { return _doc_freqs.data();       }

        Shard::packed_table_ptr table() { return _table; }

        size_t count() const;

        void reset(const int64_t id, Shard::packed_table_ptr table);
        void reset(const std::string& id, Shard::packed_table_ptr table);

        template<typename term_t>
        TermDesc& operator+=(const term_t& term);

    private:
        int64_t                 _int_term;
        std::string             _string_term;
        bool                    _is_int_type;
        std::vector<int64_t>    _docid_addresses;
        std::vector<int64_t>    _doc_freqs;
        Shard::packed_table_ptr _table;
    };


    template <typename term_t>
    class TermDescIterator {
    public:
        typedef TermDesc value_type;
        typedef MergeIterator<term_t> iterator_t;

        TermDescIterator() { }

        TermDescIterator(const iterator_t begin, const iterator_t end) :
                _begin(begin),
                _end(end)
        { }

        bool hasNext(void) const;

        void next(value_type& data);

    private:
        iterator_t _begin;
        iterator_t _end;
    };

} // namespace imhotep

#endif
