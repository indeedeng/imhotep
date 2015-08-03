#ifndef BTREE_DATA_BLOCK_HPP
#define BTREE_DATA_BLOCK_HPP

#include <cstdint>
#include <iostream>
#include <iterator>

#include "key_value.hpp"

namespace imhotep {
    namespace btree {

        /** A DataBlock looks like this:
            -----------------------------------------------------------------------------
            |        |            |     |                  |        |     |             |
            | length | offset (0) | ... | offset (len - 1) | kv (0) | ... | kv (len -1) |
            |        |            |     |                  |        |     |             |
            -----------------------------------------------------------------------------
            | begin  | offset_begin                        | kv_begin

            - length is an int32_t containing the number of elements in the block
            - offset is an int16 byte offset starting from kv_begin
            - each kv consists of a serialized key and value pair
         */
        template <typename Key, typename Value>
        class DataBlock {
        public:
            DataBlock(const char* begin=0) : _begin(begin) { }

            size_t length() const { return *reinterpret_cast<const Length*>(_begin); }

            KeyValue<Key, Value> key_value(size_t index) const {
                return KeyValue<Key, Value>(kv_begin() + offset(index));
            }

            bool operator==(const DataBlock& rhs) const { return _begin == rhs._begin; }

            size_t lower_bound(size_t first, size_t last, const Key& key) const {
                size_t index(0);
                size_t count(last - first);
                size_t step(0);
                while (count > 0) {
                    index  = first;
                    step   = count / 2;
                    index += step;
                    if (key_value(index).key() < key) {
                        first = ++index;
                        count -= step + 1;
                    }
                    else {
                        count = step;
                    }
                }
                return first;
            }

            size_t floor(const Key& key) const {
                const size_t result(lower_bound(0, length(), key));
                return result > 0 && key < key_value(result).key() ? result - 1 : result;
            }

        private:
            const char* _begin;

            typedef uint32_t Length;
            typedef uint16_t Offset;

            template <typename int_t>
            int_t as_int(const char* begin) const {
                return *reinterpret_cast<const int_t*>(begin);
            }

            const char* offset_begin() const { return _begin + sizeof(Length);                    }
            const char*     kv_begin() const { return offset_begin() + length() * sizeof(Offset); }

            size_t offset(size_t index) const {
                return as_int<Offset>(offset_begin() + index * sizeof(Offset));
            }
        };

    } // namespace btree
} // namespace imhotep

template <typename Key, typename Value>
std::ostream& operator<<(std::ostream& os, const imhotep::btree::DataBlock<Key, Value>& block) {
    os << "DataBlock {" << " length: " << block.length();
    for (size_t index(0); index < std::min(block.length(), size_t(1)); ++index) {
        os << " [" << index << "] " << block.key_value(index);
    }
    os << " ... }";
    return os;
}

#endif
