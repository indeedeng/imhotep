#include <cstdint>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>

#include "mmapped_file.hpp"

namespace imhotep {
    namespace btree {

#pragma pack(push, 1)
        class Header {
            const int32_t _index_levels;
            const int64_t _root_index_start_address;
            const int64_t _value_level_length;
            const int64_t _size;
            const uint8_t _has_deletions;
            const int64_t _file_length;
        public:
            int32_t             index_levels() const { return _index_levels;             }
            int64_t root_index_start_address() const { return _root_index_start_address; }
            int64_t       value_level_length() const { return _value_level_length;       }
            int64_t                     size() const { return _size;                     }
            bool               has_deletions() const { return _has_deletions != 0;       }
            int64_t              file_length() const { return _file_length;              }

        };
#pragma pack(pop)

        class HeaderView {
            const Header* _header;
        public:
            HeaderView(const char* begin, const char* end)
                : _header(reinterpret_cast<const Header*>(begin)) {
                if (end - begin < sizeof(Header)) {
                    throw std::invalid_argument(__PRETTY_FUNCTION__);
                }
            }

            const Header& operator()() const { return *_header; }
        };

        template <typename int_t>
        int_t as_int(const char* begin) {
            return *reinterpret_cast<const int_t*>(begin);
        }

        /** A StringView looks like this:
            ______________________________
            |        |                   |
            | length | chars             |
            |        |                   |
            ------------------------------

            where length is varint encoded, either one or five bytes.
        */
        class StringView {
            const char* _begin;
        public:
            typedef uint8_t  short_length_t;
            typedef uint32_t length_t;

            StringView(const char* begin) : _begin(begin) { }

            std::string operator()() const { return std::string(begin(), end()); }

            size_t length() const {
                return (*_begin < 0xff) ?
                    *reinterpret_cast<const short_length_t*>(_begin) :
                    *reinterpret_cast<const length_t*>(_begin + 1);
            }

            const char* begin() const {
                return (*_begin < 0xff) ?
                    _begin + sizeof(short_length_t) :
                    _begin + sizeof(length_t) + 1;
            }

            const char* end() const { return begin() + length(); }
        };

        template <typename int_t>
        class IntView {
            const char* _begin;
        public:
            IntView(const char* begin) : _begin(begin) { }

            int_t operator()() const {
                return *reinterpret_cast<const int_t*>(_begin);
            }

            size_t length() const { return sizeof(int_t); }

            const char* begin() const { return _begin;             }
            const char*   end() const { return begin() + length(); }
        };

        template <typename Key, typename Value>
        class KeyValue {
            const char* _begin;
        public:
            KeyValue(const char* begin) : _begin(begin) { }

            Key     key() const { return Key(_begin);          }
            Value value() const { return Value( key().end() ); }
        };

        /** A DataBlock looks like this:
            -----------------------------------------------------------------------------
            |        |            |     |                  |        |     |             |
            | length | offset (0) | ... | offset (len - 1) | kv (0) | ... | kv (len -1) |
            |        |            |     |                  |        |     |             |
            -----------------------------------------------------------------------------
            | begin  | offset_begin                        | kv_begin

            - length is an int32_t containing the number of elements in the block
            - offset is an int16 offset starting from kv_begin
            - each kv consists of a serialized key and value pair

         */
        template <typename Key, typename Value>
        class DataBlock {
            const char* _begin;
        public:
            DataBlock(const char* begin) : _begin(begin) { }

            size_t length() const { return *reinterpret_cast<const Length*>(_begin); }

            KeyValue<Key, Value> key_value(size_t index) const {
                return KeyValue<Key, Value>(kv_begin() + offset(index));
            }

        private:
            typedef uint32_t Length;
            typedef uint16_t Offset;

            const char* offset_begin() const { return _begin + sizeof(Length);                    }
            const char*     kv_begin() const { return offset_begin() + length() * sizeof(Offset); }

            size_t offset(size_t index) const {
                return as_int<Offset>(offset_begin() + index * sizeof(Offset));
            }
        };

        typedef IntView<uint64_t> LongView;

        template <typename Key>
        class IndexBlock : public DataBlock<Key, LongView> {
        public:
            IndexBlock(const char* begin)
                : DataBlock<Key, LongView>(begin) { }
        };

    } // namespace btree
} // namespace imhotep

inline
std::ostream& operator<<(std::ostream& os, const imhotep::btree::Header& header) {
    os << "Header {"
       << " index_levels: " << header.index_levels()
       << " root_index_start_address: " << header.root_index_start_address()
       << " value_level_length: " << header.value_level_length()
       << " size: " << header.size()
       << " has_deletions: " << header.has_deletions()
       << " file_length: " << header.file_length()
       << "}";
    return os;
}

template <typename Key, typename Value>
std::ostream& operator<<(std::ostream& os, const imhotep::btree::KeyValue<Key, Value>& kv) {
    os << kv.key()() << ":" << kv.value()();
    return os;
}

template <typename Key, typename Value>
std::ostream& operator<<(std::ostream& os, const imhotep::btree::DataBlock<Key, Value>& block) {
    os << "DataBlock {" << " length: " << block.length();
    for (size_t index(0); index < std::min(block.length(), size_t(1)); ++index) {
        os << " [" << index << "] " << block.key_value(index);
    }
    os << " ... }";
    return os;
}

template <typename Key>
std::ostream& operator<<(std::ostream& os, const imhotep::btree::IndexBlock<Key>& block) {
    os << "IndexBlock {" << " length: " << block.length();
    for (size_t index(0); index < block.length(); ++index) {
        os << " [" << index << "] " << block.key_value(index);
    }
    os << " }";
    return os;
}

using namespace imhotep;
using namespace imhotep::btree;

typedef KeyValue<StringView, LongView> IndexKV;

void dump(size_t level, const char* begin, const IndexBlock<StringView>& block, size_t indent=0) {
    for (size_t count(0); count < indent; ++count)
        std::cout << ' ';
    std::cout << block << std::endl;
    if (level > 0) {
        --level;
        for (size_t index(0); index < block.length(); ++index) {
            const IndexKV kv(block.key_value(index));
            if (level > 0) {
                const IndexBlock<StringView> ib(begin + kv.value()());
                dump(level, begin, ib, indent + 3);
            }
            else {
                const DataBlock<StringView, LongView> db(begin + kv.value()());
                for (size_t count(0); count < indent + 3; ++count)
                    std::cout << ' ';
                std::cout << db << std::endl;
            }
        }
    }
}

int main(int argc, char* argv[]) {
    std::string fname(argv[1]);

    const MMappedFile index(fname);
    const HeaderView header(index.end() - sizeof(Header), index.end());
    // !@# ulimately puke if has_deletions is true?
    std::cout << header() << std::endl;

    IndexBlock<StringView> root_block(index.begin() + header().root_index_start_address());
    dump(header().index_levels(), index.begin(), root_block);
}
