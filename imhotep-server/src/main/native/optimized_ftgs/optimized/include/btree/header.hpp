#ifndef BTREE_HEADER_HPP
#define BTREE_HEADER_HPP

#include <cstdint>
#include <iostream>

namespace imhotep {
    namespace btree {

        class Header {
        public:
            Header(const char* begin, const char* end)
                : _fields(*reinterpret_cast<const Fields*>(begin)) {
                if (end - begin < sizeof(Fields)) {
                    throw std::invalid_argument(__PRETTY_FUNCTION__);
                }
            }

            static size_t length() { return sizeof(Fields); }

            int32_t             index_levels() const { return _fields._index_levels;             }
            int64_t root_index_start_address() const { return _fields._root_index_start_address; }
            int64_t       value_level_length() const { return _fields._value_level_length;       }
            int64_t                     size() const { return _fields._size;                     }
            bool               has_deletions() const { return _fields._has_deletions != 0;       }
            int64_t              file_length() const { return _fields._file_length;              }

        private:
            #pragma pack(push, 1)
            struct Fields {
                const int32_t _index_levels;
                const int64_t _root_index_start_address;
                const int64_t _value_level_length;
                const int64_t _size;
                const uint8_t _has_deletions;
                const int64_t _file_length;
            };
            #pragma pack(pop)

            const Fields& _fields;
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

#endif
