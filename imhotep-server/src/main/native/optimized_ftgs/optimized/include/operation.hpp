/** @file operation.hpp implements states for this state transition diagram:

    FS  = field start            |
    FE  = field end              | (index, name, type) <--
    TGS = tgs operation          V                       |
    NMF = no more fields    ----(FS)                     |
                            |    |                       |
                            |    | (term desc) <--       |
                            |    V               |       |
                            |   (TGS)-------------       |
                            |    |                       |
                            |    |                       |
                            |    V                       |
                            --> (FE)----------------------
                            |
                            |
                            V
                           (NMF)
*/
#ifndef OPERATION_HPP
#define OPERATION_HPP

#include <sstream>

#include "merge_iterator.hpp"
#include "term_seq.hpp"

namespace imhotep {

    enum OpCode:int8_t { INVALID = 0, TGS = 1, FIELD_START = 2, FIELD_END = 3, NO_MORE_FIELDS = 4 };
    enum FieldType:int { UNDEFINED = -1, STR_TERM = 0, INT_TERM = 1 };

    inline
    std::string op_code_string(OpCode op_code) {
        switch (op_code) {
        case INVALID:        return "INVALID";
        case TGS:            return "TGS";
        case FIELD_START:    return "FIELD_START";
        case FIELD_END:      return "FIELD_END";
        case NO_MORE_FIELDS: return "NO_MORE_FIELDS";
        default:             return "<software error - bad opcode>";
        }
    }


    template <typename term_t>
    class Operation {
    public:
        Operation() : _op_code(INVALID) { }

        int32_t            split_index() const { return _split_index; }
        OpCode                 op_code() const { return _op_code;     }
        const std::string&  field_name() const { return _field_name;  }

        FieldType field_type() const;

        const TermSeq<term_t>& term_seq() const { return _term_seq; }

        bool operator==(const Operation& rhs) const;

        void clear();
        void field_start(int32_t split_index, const std::string& field_name, MergeIterator<term_t>& merge_it);
        void tgs(MergeIterator<term_t>& merge_it);
        void field_end(const Operation& operation);
        void no_more_fields(int32_t split_index);

        std::string to_string() const;

    private:
        OpCode          _op_code     = INVALID;
        int32_t         _split_index = 0;
        std::string     _field_name;
        TermSeq<term_t> _term_seq;
    };

    template<> inline
    FieldType Operation<IntTerm>::Operation::field_type() const { return INT_TERM; }

    template<> inline
    FieldType Operation<StringTerm>::Operation::field_type() const { return STR_TERM; }

} // namespace imhotep

#endif
