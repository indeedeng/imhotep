#ifndef BTREE_INDEX_BLOCK_HPP
#define BTREE_INDEX_BLOCK_HPP

#include <algorithm>
#include <cstdint>
#include <iostream>

#include "data_block.hpp"
#include "header.hpp"
#include "int.hpp"

namespace imhotep {
    namespace btree {

        template <typename Key>
        class IndexBlock : public DataBlock<Key, Long> {
        public:
            IndexBlock(const char* begin) : DataBlock<Key, Long>(begin) { }

            template <typename Value>
            KeyValue<Key, Value>
            find(const char* base_address, const Key& key, size_t level) const {
                const size_t child_index(this->floor(key));
                const size_t child_offset(this->key_value(child_index).value()());
                const char*  child_begin(base_address + child_offset);
                const size_t child_level(level - 1);
                if (child_level > 0) {
                    const IndexBlock<Key> child(child_begin);
                    return child.find<Value>(base_address, key, child_level);
                }
                else {
                    const DataBlock<Key, Value> child(child_begin);
                    return child.find(key);
                }
            }
        };

    } // namespace btree
} // namespace imhotep

template <typename Key>
std::ostream& operator<<(std::ostream& os, const imhotep::btree::IndexBlock<Key>& block) {
    os << "IndexBlock {" << " length: " << block.length();
    for (size_t index(0); index < block.length(); ++index) {
        os << " [" << index << "] " << block.key_value(index);
    }
    os << " }";
    return os;
}

#endif
