#ifndef BTREE_DATA_BLOCK_ITERATOR_HPP
#define BTREE_DATA_BLOCK_ITERATOR_HPP

#include <iterator>

#include <boost/iterator/iterator_facade.hpp>

#include "data_block.hpp"

namespace imhotep {
    namespace btree {

        /** A random access iterator over the KeyValue pairs within a DataBlock.
         */
        template <typename Key, typename Value>
        class DataBlockIterator
            : public boost::iterator_facade<DataBlockIterator<Key, Value>,
                                            KeyValue<Key, Value> const,
                                            boost::forward_traversal_tag> {
        public:
            DataBlockIterator() : _index(0) { }
            DataBlockIterator(const DataBlock<Key, Value>& block) : _block(block), _index(0) { }
        private:
            friend class boost::iterator_core_access;

            void increment() {
                ++_index;
                if (_index >= _block.length()) {
                    *this = DataBlock<Key, Value>();
                }
            }
            void decrement() { if (_index > 0) --_index; }

            void advance(size_t n) {
                _index += n;
                if (_index >= _block.length()) {
                    *this = DataBlock<Key, Value>();
                }
            }

            size_t distance_to(const DataBlockIterator& rhs) const {
                return std::distance(_index, rhs._index);
            }

            bool equal(const DataBlockIterator<Key, Value>& rhs) const {
                return _block == rhs._block && _index == rhs._index;
            }

            const KeyValue<Key, Value> dereference() const {
                return (_index < _block.length()) ?
                    _block.key_value(_index) : KeyValue<Key, Value>();
            }

            DataBlock<Key, Value> _block;
            size_t                _index;
        };

    } // namespace btree
} // namespace imhotep

#endif
