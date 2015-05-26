#ifndef MERGE_ITERATOR_HPP
#define MERGE_ITERATOR_HPP

#include <algorithm>
#include <queue>
#include <utility>
#include <vector>

#include <boost/iterator/iterator_facade.hpp>

#include "shard.hpp"
#include "split_iterator.hpp"

namespace imhotep {

    template <typename term_t>
    class MergeIterator
        : public boost::iterator_facade<MergeIterator<term_t>,
                                        std::pair<term_t, Shard::packed_table_ptr> const,
                                        boost::forward_traversal_tag> {
    public:
        typedef std::pair<SplitIterator<term_t>, Shard::packed_table_ptr> Entry;

        MergeIterator() { }

        template <typename iterator>
        MergeIterator(iterator begin, iterator end)
            : _its(begin, end)
            , _queue(CompareIt(), _its) {
            // !@# todo(johnf) : only add iterators that are not at their ends...
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        struct CompareIt {
            bool operator()(const Entry& thing1,
                            const Entry& thing2) {
                return *thing1.first > *thing2.first;
            }
        };

        typedef std::priority_queue<
            Entry,
            std::vector<Entry>,
            CompareIt
            > PriorityQueue;

        void increment() {
            static SplitIterator<term_t> end;

            if (_queue.empty()) {
                _its.clear();
                return;
            }

            Entry entry(_queue.top());
            _current = std::make_pair(*entry.first, entry.second);
            _queue.pop();
            ++entry.first;
            if (entry.first != end) {
                _queue.push(entry);
            }
        }

        bool equal(const MergeIterator& other) const {
            return _its == other._its;
        }

        typename MergeIterator::reference dereference() const {
            return _current;
        }

        std::vector<Entry> _its;

        PriorityQueue                      _queue;
        typename MergeIterator::value_type _current;
    };
}

#endif
