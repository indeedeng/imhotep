#ifndef STRING_RANGE_HPP
#define STRING_RANGE_HPP

#include <string>
#include <utility>

namespace imhotep {

    class StringRange : public std::pair<const char*, const char*> {
    public:
        explicit StringRange(const std::string& str)
            : std::pair<const char*, const char*>(std::make_pair(str.c_str(),
                                                                 str.c_str() + str.size()))
        { }

        explicit StringRange(const char* begin=0, const char* end=0)
            : std::pair<const char*, const char*>(std::make_pair(begin, end))
        { }

        const char* begin() const { return first;  }
        const char*   end() const { return second; }

        char const* c_str() const { return first; }

        size_t size() const { return std::distance(first, second); }

        bool empty() const { return size() == 0; }

        bool operator==(const StringRange& rhs) const {
            if (size() != rhs.size()) return false;
            if (begin() == rhs.begin()) return true;
            return std::equal(begin(), end(), rhs.begin());
        }

        bool operator!=(const StringRange& rhs) const {
            if (size() == rhs.size()) return false;
            return !std::equal(begin(), end(), rhs.begin());
        }

        bool operator<(const StringRange& rhs)  const {
            const size_t len(std::min(size(), rhs.size()));
            const int result(strncmp(begin(), rhs.begin(), len));
            return
                result < 0 ? true  :
                result > 0 ? false :
                size() < rhs.size();
        }

        bool operator>(const StringRange& rhs)  const = delete;
        bool operator<=(const StringRange& rhs) const = delete;
        bool operator>=(const StringRange& rhs) const = delete;

        explicit operator std::string() const {
            return std::string(begin(), end());
        }
    };

} // namespace imhotep

#endif
