#ifndef TERM_SEQ_ITERATOR_HPP
#define TERM_SEQ_ITERATOR_HPP

#include <algorithm>
#include <utility>
#include <vector>

#include <boost/iterator/iterator_facade.hpp>

#include "merge_iterator.hpp"
#include "shard.hpp"
#include "term_seq.hpp"

namespace imhotep {

    template <typename term_t>
    class TermSeqIterator
        : public boost::iterator_facade<TermSeqIterator<term_t>,
                                        TermSeq<term_t> const,
                                        boost::forward_traversal_tag> {
    public:
        TermSeqIterator() { }

        TermSeqIterator(MergeIterator<term_t> begin, MergeIterator<term_t> end)
            : _current(begin)
            , _end(end) {
            increment();
        }

    private:
        friend class boost::iterator_core_access;

        void increment() {
            if (_current != _end) {
                const typename term_t::id_t id((*_current)._term.id());
                auto matches_id([id](MergeOutput<term_t> element) {
                        return element._term.id() == id;
                    });

                MergeIterator<term_t> next(std::find_if_not(_current, _end, matches_id));
                _term_seq.reset(_current, next);
                _current = next;
            }
            else {
                _term_seq = TermSeq<term_t>();
            }
        }

        bool equal(const TermSeqIterator& other) const {
            return _current == other._current &&
                _term_seq == other._term_seq;
        }

        const TermSeq<term_t>& dereference() const { return _term_seq; }

        TermSeq<term_t> _term_seq;

        MergeIterator<term_t> _current;
        MergeIterator<term_t> _end;
    };

} // namespace imhotep

#endif
