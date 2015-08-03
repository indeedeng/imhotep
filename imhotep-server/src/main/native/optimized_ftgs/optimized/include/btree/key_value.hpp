#ifndef BTREE_KEY_VALUE_HPP
#define BTREE_KEY_VALUE_HPP

#include <iostream>

namespace imhotep {
    namespace btree {

        template <typename Key, typename Value>
        class KeyValue {
            const char* _begin;
        public:
            KeyValue(const char* begin=0) : _begin(begin) { }

            Key     key() const { return Key(_begin);          }
            Value value() const { return Value( key().end() ); }
        };

    } // namespace btree
} // namespace imhotep

template <typename Key, typename Value>
std::ostream& operator<<(std::ostream& os, const imhotep::btree::KeyValue<Key, Value>& kv) {
    os << kv.key()() << ":" << kv.value();
    return os;
}

#endif
