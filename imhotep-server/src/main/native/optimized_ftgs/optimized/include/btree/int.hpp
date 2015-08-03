#ifndef BTREE_INT_HPP
#define BTREE_INT_HPP

#include <cstdint>
#include <iostream>
#include <utility>

namespace imhotep {
    namespace btree {

        template <typename int_t>
        class Int {
            const char* _begin;
        public:
            Int(const char* begin) : _begin(begin) { }

            int_t operator()() const {
                return *reinterpret_cast<const int_t*>(_begin);
            }

            bool operator<(const int_t& rhs) const {
                return *this() < rhs();
            }

            size_t length() const { return sizeof(int_t); }

            const char* begin() const { return _begin;             }
            const char*   end() const { return begin() + length(); }
        };

        typedef Int<int64_t> Long;

        typedef Int<std::pair<int64_t, int64_t> > LongPair; // !@# fix operator< for this

} // namespace btree
} // namespace imhotep

template <typename int_t>
std::ostream& operator<<(std::ostream& os, const imhotep::btree::Int<int_t>& value) {
    os << value();
    return os;
}

inline std::ostream&
operator<<(std::ostream& os, const imhotep::btree::LongPair& value) {
    os << "(" << value().first << " . " << value().second << ")";
    return os;
}

#endif
