#include "operation.hpp"

namespace imhotep {

    template <typename term_t>
    bool Operation<term_t>::operator==(const Operation& rhs) const {
        return
            _op_code     == rhs._op_code     &&
            _split_index == rhs._split_index &&
            _field_name  == rhs._field_name;
    }

    template <typename term_t>
    void Operation<term_t>::clear() {
        _op_code = INVALID;
        _split_index = 0;
        _field_name.clear();
        _term_seq.reset();
    }

    template <typename term_t>
    void Operation<term_t>::field_start(int32_t split_index,
                                        const std::string& field_name,
                                        MergeIterator<term_t>& merge_it) {
        _op_code     = FIELD_START;
        _split_index = split_index;
        _field_name  = field_name;
        _term_seq.reset();
    }

    template <typename term_t>
    void Operation<term_t>::tgs(MergeIterator<term_t>& merge_it) {
        _op_code = TGS;
        static const MergeIterator<term_t> merge_end;
        if (merge_it != merge_end) {
            const typename term_t::id_t id((*merge_it)._term.id());
            _term_seq.reset(id);
            while (merge_it != merge_end && (*merge_it)._term.id() == id) {
                _term_seq.push_back(*merge_it);
                ++merge_it;
            }
        }
        else {
            _term_seq.reset();
        }
    }

    template <typename term_t>
    void Operation<term_t>::field_end(const Operation& operation) {
        assert(operation.op_code() == FIELD_START || operation.op_code() == TGS);
        _op_code = FIELD_END;
    }

    template <typename term_t>
    void Operation<term_t>::no_more_fields(int32_t split_index) {
        _op_code = NO_MORE_FIELDS;
        _term_seq.reset();
    }

    template <typename term_t>
    std::string Operation<term_t>::to_string() const {
        std::ostringstream os;
        os << "[Operation split_index=" << split_index()
           << " op_code=" << op_code() << "/" << op_code_string(op_code())
           << " field_name=" << field_name()
           << " term_seq=" << term_seq().to_string()
           << "]";
        return os.str();
    }


    /* template instantiations */
    template bool Operation<IntTerm>::operator==(const Operation& rhs) const;
    template void Operation<IntTerm>::clear();
    template void Operation<IntTerm>::field_start(int32_t split_index, const std::string& field_name,
                                                  MergeIterator<IntTerm>& merge_it);
    template void Operation<IntTerm>::tgs(MergeIterator<IntTerm>& merge_it);
    template void Operation<IntTerm>::field_end(const Operation& operation);
    template void Operation<IntTerm>::no_more_fields(int32_t split_index);
    template std::string Operation<IntTerm>::to_string() const;

    template bool Operation<StringTerm>::operator==(const Operation& rhs) const;
    template void Operation<StringTerm>::clear();
    template void Operation<StringTerm>::field_start(int32_t split_index, const std::string& field_name,
                                                     MergeIterator<StringTerm>& merge_it);
    template void Operation<StringTerm>::tgs(MergeIterator<StringTerm>& merge_it);
    template void Operation<StringTerm>::field_end(const Operation& operation);
    template void Operation<StringTerm>::no_more_fields(int32_t split_index);
    template std::string Operation<StringTerm>::to_string() const;

} // namespace imhotep