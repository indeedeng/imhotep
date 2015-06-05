#ifndef TERM_DESC_HPP
#define TERM_DESC_HPP

#include <cstdint>
#include <string>
#include <vector>

#include "shard.hpp"

namespace imhotep {

    class TermDesc {
    public:
        int64_t               int_term() const { return _int_term;    }
        const std::string& string_term() const { return _string_term; }

        const int64_t*           docid_addresses() const { return _docid_addresses.data(); }
        const int32_t*                 doc_freqs() const { return _doc_freqs.data();       }
        const Shard::packed_table_ptr*    tables() const { return _tables.data();          }

        const bool is_int_field() const { return _is_int_type; }

        size_t count() const;

        void reset(const int64_t id);
        void reset(const std::string& id);

        template<typename term_t>
        TermDesc& append(const term_t& term, Shard::packed_table_ptr table);

        Encoding encode_type() const {
            return _is_int_type ? INT_TERM_TYPE : STRING_TERM_TYPE;
        }

    private:
        int64_t                              _int_term;
        std::string                          _string_term;
        std::vector<int64_t>                 _docid_addresses;
        std::vector<int32_t>                 _doc_freqs;
        std::vector<Shard::packed_table_ptr> _tables;
        bool                                 _is_int_type;
    };


} // namespace imhotep

#endif
