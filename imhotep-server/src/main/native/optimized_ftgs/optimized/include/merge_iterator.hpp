#ifndef MERGE_ITERATOR_HPP
#define MERGE_ITERATOR_HPP

#include <algorithm>
#include <tuple>
#include <utility>
#include <vector>

#include <boost/iterator/iterator_facade.hpp>

#include "log.hpp"
#include "shard.hpp"
#include "split_iterator.hpp"

namespace imhotep {

    template <typename term_t>
    struct MergeInput {
        SplitIterator<term_t>   _split_it;
        Shard::packed_table_ptr _table;
        const char*             _docid_base;

        MergeInput(const SplitIterator<term_t>& split_it,
                   Shard::packed_table_ptr      table,
                   const char*                  docid_base)
            : _split_it(split_it)
            , _table(table)
            , _docid_base(docid_base)
        { }

        bool operator==(const MergeInput& rhs) const {
            return _split_it == rhs._split_it;
        }
    };

    template <typename term_t>
    struct MergeOutput {
        term_t                  _term;
        Shard::packed_table_ptr _table      = 0;
        const char*             _docid_base = 0;

        MergeOutput() { }

        MergeOutput(const term_t&           term,
                    Shard::packed_table_ptr table,
                    const char*             docid_base)
            : _term(term)
            , _table(table)
            , _docid_base(docid_base)
        { }

        MergeOutput(const MergeInput<term_t>& input)
            : _term(*input._split_it)
            , _table(input._table)
            , _docid_base(input._docid_base)
        { }

        bool operator==(const MergeOutput& rhs) const {
            return
                _table      == rhs._table &&
                _docid_base == rhs._docid_base &&
                _term       == rhs._term;
        }
    };

    /** Combine a collection of SplitIterators such that we can iterate through
        them collectively in ascending term id order.
     */
    template <typename term_t>
    class MergeIterator
        : public boost::iterator_facade<MergeIterator<term_t>,
                                        MergeOutput<term_t> const,
                                        boost::forward_traversal_tag> {
    public:
        MergeIterator() { }

        template <typename iterator>
        MergeIterator(iterator begin, iterator end) {
            std::copy_if(begin, end, std::back_inserter(_its),
                         [] (MergeInput<term_t>& entry) {
                             const SplitIterator<term_t> split_end;
                             return entry._split_it != split_end;
                         });
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        /* Use a simple linear scan to find and increment the SplitIterator
           referring to the Term with the lowest id. Throw it away if it's
           empty. Note that a linear scan beats the pants off of a heap for
           small numbers of contained iterators. */
        void increment() {
            if (!_its.empty()) {
                const SplitIterator<term_t> end;

                auto lowest(_its.begin());
                for (auto it(_its.begin()); it != _its.end(); ++it) {
                    const MergeInput<term_t>& entry(*it);
                    if (*entry._split_it < *lowest->_split_it) {
                        lowest = it;
                    }
                }

                MergeInput<term_t>& entry(*lowest);
                _current = MergeOutput<term_t>(entry);
                ++entry._split_it;
                if (entry._split_it == end) _its.erase(lowest);
            }
            else {
                _current = MergeOutput<term_t>();
            }
        }

        bool equal(const MergeIterator& other) const {
            return
                _current == other._current &&
                _its == other._its;
        }

        const MergeOutput<term_t>& dereference() const { return _current; }

        std::vector<MergeInput<term_t>> _its;

        MergeOutput<term_t> _current;
    };
}

#endif
