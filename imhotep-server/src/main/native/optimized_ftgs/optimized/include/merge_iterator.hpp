#ifndef MERGE_ITERATOR_HPP
#define MERGE_ITERATOR_HPP

#include <algorithm>
#include <utility>
#include <vector>

#include <boost/iterator/iterator_facade.hpp>

#include "shard.hpp"
#include "split_iterator.hpp"

namespace imhotep {

    /** Combine a collection of SplitIterators such that we can iterate through
        them collectively in ascending term id order.

        Performance note: A vector organized as a heap is used internally to
        order the SplitIterators. For smaller collections this loses out to a
        simple linear scan approach. I'm taking it on faith that the heap will
        win for larger collections, but this is worth revisiting in a more
        realistic test scenario.
     */
    template <typename term_t>
    class MergeIterator
        : public boost::iterator_facade<MergeIterator<term_t>,
                                        std::pair<term_t, Shard::packed_table_ptr> const,
                                        boost::forward_traversal_tag> {
    public:
        typedef std::pair<SplitIterator<term_t>, Shard::packed_table_ptr> Entry;

        MergeIterator() { }

        template <typename iterator>
        MergeIterator(iterator begin, iterator end) {
            std::copy_if(begin, end, std::back_inserter(_its),
                         [] (Entry& entry) {
                             static SplitIterator<term_t> split_end;
                             return entry.first != split_end;
                         });
            std::make_heap(_its.begin(), _its.end(), _compare);
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        struct CompareIt {
            bool operator()(const Entry& thing1,
                            const Entry& thing2) const {
                return *thing1.first > *thing2.first;
            }
        };

        void increment() {
            static const SplitIterator<term_t> end;

            if (_its.empty()) return;

            auto lowest(_its.begin());
            Entry& entry(*lowest);
            _current = std::make_pair(*entry.first, entry.second);

            ++entry.first;
            if (entry.first == end) {
                _its.erase(lowest);
            }
            else {
                std::pop_heap(_its.begin(), _its.end(), _compare);
                std::push_heap(_its.begin(), _its.end(), _compare);
            }
        }

        bool equal(const MergeIterator& other) const {
            return _its == other._its;
        }

        typename MergeIterator::reference dereference() const {
            return _current;
        }

        static const CompareIt _compare;

        std::vector<Entry> _its;

        typename MergeIterator::value_type _current;
    };
}

#endif
