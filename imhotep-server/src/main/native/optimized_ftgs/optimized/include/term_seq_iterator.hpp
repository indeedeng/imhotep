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
        typedef MergeIterator<term_t> merge_it;

        TermSeqIterator() { }

        TermSeqIterator(merge_it begin, merge_it end)
            : _current(begin)
            , _end(end)
        { }

    private:
        friend class boost::iterator_core_access;

        void increment() {
            if (_current == _end) return;

            const typename term_t::id_t id(_current->first.id());
            auto matches_id([id](typename merge_it::reference element) {
                    const term_t& term(element.first);
                    return term.id() == id;
                });

            merge_it next(std::find_if_not(_current, _end, matches_id));
            _term_seq = TermSeq<term_t>(_current, next);
            _current = next;
        }

        bool equal(const TermSeqIterator& other) const {
            return _current == other._current; // !@# re-examine
        }

        const TermSeq<term_t>& dereference() const { return _term_seq; }

        TermSeq<term_t> _term_seq;

        merge_it _current;
        merge_it _end;
    };

} // namespace imhotep

#endif
