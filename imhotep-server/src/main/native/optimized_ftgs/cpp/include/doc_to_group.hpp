#ifndef DOC_TO_GROUP_HPP
#define DOC_TO_GROUP_HPP

#include <cstdint>

#define restrict __restrict__
extern "C" {
#include "imhotep_native.h"
#include "table.h"
}

/* Notes to self:

   - Unfortunately, packed_table is not too hygenic wrt
     signed/unsigned types. For example, packed_table_get_rows()
     should never actually return a negative number even though its
     return type is a signed int. For that reason, the code below is
     littered with static_casts. :-(
*/
namespace imhotep {

    class DocToGroup {
    public:
        typedef void* table_ptr_t; // !@# blech!

        typedef uint32_t doc_t;
        typedef int32_t  group_t;

        DocToGroup(table_ptr_t table)
            : _table(table)
            , _end(end(table))
        { }

        group_t get(doc_t doc) const
        {
            ::packed_table_ptr table(reinterpret_cast<::packed_table_ptr>(_table));
            const int32_t      row(static_cast<int32_t>(doc));
            return packed_table_get_group(table, row);
        }

        void set(doc_t doc, group_t group)
        {
            ::packed_table_ptr table(reinterpret_cast<::packed_table_ptr>(_table));
            const int32_t      row(static_cast<int32_t>(doc));
            packed_table_set_group(table, row, group);
        }

        doc_t begin() const { return 0;    }
        doc_t   end() const { return _end; }

        size_t num_groups() const {
            ::packed_table_ptr table(reinterpret_cast<::packed_table_ptr>(_table));
            return static_cast<size_t>(packed_table_get_num_groups(table));
        }

    private:
        table_ptr_t _table;
        const doc_t _end;

        static doc_t end(table_ptr_t table_ptr) {
            ::packed_table_ptr table(reinterpret_cast<::packed_table_ptr>(table_ptr));
            return static_cast<DocToGroup::doc_t>(packed_table_get_rows(table));
        }
    };

} // namespace imhotep

#endif
