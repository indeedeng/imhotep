#ifndef TERM_SEQ_STATE
#define TERM_SEQ_STATE

#include <cstdint>
#include <vector>

#include "shard.hpp"

namespace imhotep {

    struct TermSeqState {
        std::vector<const char*>             _docid_addresses;
        std::vector<int32_t>                 _doc_freqs;
        std::vector<Shard::packed_table_ptr> _tables;

        void clear() {
            _docid_addresses.clear();
            _doc_freqs.clear();
            _tables.clear();
        }
    };

} // namespace imhotep

#endif
