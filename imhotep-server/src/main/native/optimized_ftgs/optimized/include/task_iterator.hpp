#ifndef TASK_ITERATOR_HPP
#define TASK_ITERATOR_HPP

#include <functional>
#include <sstream>

#include <boost/iterator/iterator_facade.hpp>

#include "field_op_iterator.hpp"
#include "imhotep_error.hpp"
#include "log.hpp"
#include "term.hpp"
#include "term_providers.hpp"

extern "C" {
#include "local_session.h"
#include "imhotep_native.h"
}

namespace imhotep {

    class TaskIterator
        : public boost::iterator_facade<TaskIterator,
                                        int const,
                                        boost::forward_traversal_tag> {
    public:
        TaskIterator();

        TaskIterator(struct worker_desc*              worker,
                     struct session_desc*             session,
                     size_t                           split,
                     size_t                           worker_id,
                     const TermProviders<IntTerm>&    int_providers,
                     const TermProviders<StringTerm>& str_providers);
    private:
        friend class boost::iterator_core_access;

        template <typename term_t>
        int start_field(const Operation<term_t>& op) {
            return worker_start_field(_worker,
                                      op.field_name().c_str(),
                                      op.field_name().length(),
                                      op.field_type(),
                                      _worker_id);
        }

        template <typename term_t>
        int tgs(const Operation<term_t>& op);

        int end_field() { return worker_end_field(_worker, _worker_id); }

        int end_stream() { return worker_end_stream(_worker, _worker_id); }

        void increment();

        bool equal(const TaskIterator& other) const;

        reference dereference() const { return _err; }

        bool complete() const;

        int _err = 0;

        struct worker_desc*  _worker  = nullptr;
        struct session_desc* _session = nullptr;

        size_t _split     = 0;
        size_t _worker_id = 0;

        FieldOpIterator<IntTerm> _int_current;
        FieldOpIterator<IntTerm> _int_end;

        FieldOpIterator<StringTerm> _str_current;
        FieldOpIterator<StringTerm> _str_end;

        bool _stream_ended = false;
    };

} // namespace imhotep

#endif
