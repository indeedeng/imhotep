#ifndef OPERATION_HPP
#define OPERATION_HPP

#include "term_seq.hpp"

namespace imhotep {

    /** Implements this state transition diagram:

        FS  = field start         |
        FE  = field end           | (index, name, type) <--
        TGS = tgs operation       V                       |
        NMF = no more fields     (FS)                     |
                                  |                       |
                                  | (term desc) <--       |
                                  V               |       |
                                 (TGS)-------------       |
                                  |                       |
                                  |                       |
                                  V                       |
                                 (FE)----------------------
                                  |
                                  |
                                  V
                                 (NMF)
     */
    enum OpCode:int8_t { INVALID = 0, TGS = 1, FIELD_START = 2, FIELD_END = 3, NO_MORE_FIELDS = 4 };
    enum FieldType:int { UNDEFINED = -1, STR_TERM = 0, INT_TERM = 1 };

    template <typename term_t>
    class Operation {
    public:
        Operation() : _op_code(INVALID) { }

        bool operator==(const Operation& rhs) const {
            return
                _op_code     == rhs._op_code     &&
                _split_index == rhs._split_index &&
                _field_name  == rhs._field_name  &&
                _term_seq    == rhs._term_seq;
        }

        static Operation field_start(int32_t split_index, const std::string& field_name) {
            return Operation(split_index, field_name);
        }

        static Operation tgs(const Operation& operation, const TermSeq<term_t>& term_seq) {
            return Operation(operation, term_seq);
        }

        static Operation field_end(const Operation& operation) {
            return Operation(operation, FIELD_END);
        }

        static Operation no_more_fields(int32_t split_index) {
            return Operation(split_index);
        }

        int32_t            split_index() const { return _split_index; }
        OpCode                 op_code() const { return _op_code;     }
        const std::string&  field_name() const { return _field_name;  }

        FieldType field_type() const; // !@# rename to "term_type?"

        const TermSeq<term_t>& term_seq() const { return _term_seq; }

    private:
        Operation(int32_t split_index, const std::string& field_name)
            : _op_code(FIELD_START)
            , _split_index(split_index)
            , _field_name(field_name)
        { }

        Operation(const Operation& operation, const TermSeq<term_t>& term_seq)
            : _op_code(TGS)
            , _split_index(operation._split_index)
            , _field_name(operation._field_name)
            , _term_seq(term_seq) {
            assert(operation.op_code() == FIELD_START ||
                   operation.op_code() == TGS);
        }

        Operation(const Operation& operation, OpCode op_code)
            : _op_code(op_code)
            , _split_index(operation._split_index)
            , _field_name(operation._field_name)
            , _term_seq(operation._term_seq) {
            assert(operation.op_code() == TGS);
        }

        Operation(int32_t split_index)
            : _op_code(NO_MORE_FIELDS)
            , _split_index(split_index)
        { }

        OpCode      _op_code     = INVALID;
        int32_t     _split_index = -1;
        std::string _field_name;

        TermSeq<term_t> _term_seq;
    };

    template<> inline
    FieldType Operation<IntTerm>::Operation::field_type() const { return INT_TERM; }

    template<> inline
    FieldType Operation<StringTerm>::Operation::field_type() const { return STR_TERM; }

} // namespace imhotep

#endif
