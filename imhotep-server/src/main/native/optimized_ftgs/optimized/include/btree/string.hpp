#ifndef BTREE_STRING_HPP
#define BTREE_STRING_HPP

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <vector>

namespace imhotep {
    namespace btree {

        /** A String looks like this:
            ______________________________
            |        |                   |
            | length | chars             |
            |        |                   |
            ------------------------------

            where length is varint encoded, either one or five bytes.
        */
        class String {
            const char* _begin;
        public:
            typedef uint8_t short_length_t;
            typedef int32_t length_t;

            String(const char* begin) : _begin(begin) { }

            std::string operator()() const { return std::string(begin(), end()); }

            bool operator<(const String& rhs) const {
                return std::lexicographical_compare(begin(), end(), rhs.begin(), rhs.end());
            }

            bool has_short_length() const { return short_length_t(*_begin) < 0xffL; }

            size_t length() const {
                return has_short_length() ?
                    *reinterpret_cast<const short_length_t*>(_begin) :
                    *reinterpret_cast<const length_t*>(_begin + 1);
            }

            const char* begin() const {
                return has_short_length() ?
                    _begin + sizeof(short_length_t) :
                    _begin + sizeof(length_t) + 1;
            }

            const char* end() const { return begin() + length(); }

            /* !@# consider splitting length encoding into separate method */
            static std::vector<char> encode(const std::string& value) {
                std::vector<char> result;

                const length_t length(value.length());
                const size_t sizeof_length(length < 0xffL ? sizeof(short_length_t) :
                                           sizeof(short_length_t) + sizeof(length_t));
                result.reserve(sizeof_length + length);
                if (value.length() < 0xff) {
                    result.push_back(value.length() & 0xff);
                }
                else {
                    result.push_back(char(0xff));
                    const char* begin(reinterpret_cast<const char*>(&length));
                    const char* end(begin + sizeof(length_t));
                    std::copy(begin, end, std::back_inserter(result));
                }

                copy(value.begin(), value.end(), std::back_inserter(result));

                return result;
            }
        };

    } // namespace btree
} // namespace imhotep

inline std::ostream&
operator<<(std::ostream& os, const imhotep::btree::String& value) {
    os << value();
    return os;
}

#endif
