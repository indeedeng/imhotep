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

        const int64_t*           docid_addresses() const { return _docid_addresses.data(); }
        const int32_t*                 doc_freqs() const { return _doc_freqs.data();       }
        Shard::packed_table_ptr*          tables()       { return _tables.data();          }

        const bool is_int_field() const { return _is_int_type; }

        size_t count() const;

        void reset(const int64_t id);
        void reset(const std::string& id);

        template<typename term_t>
        TermDesc& append(const term_t& term, Shard::packed_table_ptr table);

    private:
        int64_t                              _int_term;
        std::string                          _string_term;
        std::vector<int64_t>                 _docid_addresses;
        std::vector<int32_t>                 _doc_freqs;
        std::vector<Shard::packed_table_ptr> _tables;
        bool                                 _is_int_type;
    };


    template <typename term_t>
    class TermDescIterator {
    public:
        typedef TermDesc value_type;
        typedef MergeIterator<term_t> iterator_t;

        TermDescIterator() { }

        TermDescIterator(const iterator_t begin, const iterator_t end)
            : _begin(begin)
            , _end(end)
        { }

        bool hasNext(void) const;

        void next(value_type& data);

    private:
        iterator_t _begin;
        iterator_t _end;
    };

} // namespace imhotep

#endif
