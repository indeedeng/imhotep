#ifndef OPERATION_HPP
#define OPERATION_HPP

#include <boost/optional.hpp>

#include "term_desc.hpp"

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
    class Operation {
    public:
        enum OpCode:int8_t { INVALID = 0, TGS = 1, FIELD_START = 2, FIELD_END = 3, NO_MORE_FIELDS = 4 };
        enum FieldType:int { UNDEFINED = -1, STR_TERM = 0, INT_TERM = 1 };

        Operation(int32_t split_index, const std::string& field_name, FieldType field_type)
            : _op_code(FIELD_START)
            , _split_index(split_index)
            , _field_name(field_name)
            , _field_type(field_type)
        { }

        Operation(const Operation& operation, const TermDesc& term_desc)
            : _op_code(TGS)
            , _split_index(operation._split_index)
            , _field_name(operation._field_name)
            , _field_type(operation._field_type)
            , _term_desc(term_desc) {
            assert(operation.op_code() == FIELD_START ||
                   operation.op_code() == TGS);
        }

        Operation(const Operation& operation)
            : _op_code(FIELD_END)
            , _split_index(operation._split_index)
            , _field_name(operation._field_name)
            , _field_type(operation._field_type)
            , _term_desc(operation._term_desc) {
            assert(operation.op_code() == TGS);
        }

        Operation(int32_t split_index)
            : _op_code(NO_MORE_FIELDS)
            , _split_index(split_index)
        { }

        int32_t            split_index() const { return _split_index; }
        OpCode                 op_code() const { return _op_code;     }
        FieldType           field_type() const { return _field_type;  }
        const std::string&  field_name() const { return _field_name;  }

        const boost::optional<TermDesc>& term_desc() const { return _term_desc; }

    private:
        OpCode                    _op_code     = INVALID;
        int32_t                   _split_index = -1;
        std::string               _field_name;
        FieldType                 _field_type  = UNDEFINED;
        boost::optional<TermDesc> _term_desc;
    };

} // namespace imhotep

#endif
