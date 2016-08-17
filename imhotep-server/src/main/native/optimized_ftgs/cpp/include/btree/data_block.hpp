#ifndef BTREE_DATA_BLOCK_HPP
#define BTREE_DATA_BLOCK_HPP

#include "key_value.hpp"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <iterator>

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

            size_t length() const {
                return _begin ? *reinterpret_cast<const Length*>(_begin) : 0;
            }

            size_t begin() const { return 0;        }
            size_t   end() const { return length(); }

            KeyValue<Key, Value> key_value(size_t index) const {
                return KeyValue<Key, Value>(kv_begin() + offset(index));
            }

            bool operator==(const DataBlock& rhs) const { return _begin == rhs._begin; }

            /** Return the index of the first item with a key greater than this
                one or end (i.e. length()) if none exists. */
            size_t lower_bound(size_t first, size_t last, const Key& key) const;

            /** Return the index of the item with the greatest key less than or
                equal to the one given or -1 if none exists. */
            ssize_t floor(const Key& key) const;

            /** Return an entry for a key or a nil KeyValue if none is found in
                this data block. */
            KeyValue<Key, Value> find(const Key& key) const;

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

        template <typename Key, typename Value>
        size_t DataBlock<Key, Value>::lower_bound(size_t first, size_t last, const Key& key) const {
            /* !@# TODO(johnf): fix this to leverage DataBlockIterator and std::lower_bound */
            size_t result(first);
            while (result < last) {
                if (key_value(result).key() > key) {
                    return result;
                }
                ++result;
            }
            return result;
        }

        template <typename Key, typename Value>
        ssize_t DataBlock<Key, Value>::floor(const Key& key) const {
            const size_t result(lower_bound(0, end(), key));
            return result - 1;
        }

        template <typename Key, typename Value>
        KeyValue<Key, Value> DataBlock<Key, Value>::find(const Key& key) const {
            static const KeyValue<Key, Value> nil;
            const ssize_t index(floor(key));
            return index < 0 ? nil : key_value(index);
        }

    } // namespace btree
} // namespace imhotep

template <typename Key, typename Value>
std::ostream& operator<<(std::ostream& os, const imhotep::btree::DataBlock<Key, Value>& block) {
    os << "DataBlock {" << " length: " << block.length();
    for (size_t index(0); index < block.length(); ++index) {
        os << " [" << index << "] " << block.key_value(index);
    }
    os << " }";
    return os;
}

#endif
