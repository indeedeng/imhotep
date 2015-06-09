#ifndef SHARD_HPP
#define SHARD_HPP

#include <map>
#include <memory>
#include <string>

#define restrict __restrict__
extern "C" {
#include "imhotep_native.h"
#include "table.h"
}

#include "term.hpp"
#include "var_int_view.hpp"

namespace imhotep {

    class Shard {
    public:
        /** A packed_table_ptr should be treated as a simple datum in the
            context of this class and users of it. I.e., these are created
            externally and their lifecycle managed elsewhere. (I'm not aware of
            a C++ pointer idiom appropriate for such a case or I'd use it.) */
        typedef const packed_table_t* packed_table_ptr;

        typedef std::shared_ptr<MMappedVarIntView> var_int_view_ptr;

        explicit Shard(const std::string& dir,
                       packed_table_ptr   table = packed_table_ptr())
            : _dir(dir)
            , _table(table)
        { }

        Shard(const Shard& rhs) = default;

        Shard& operator=(const Shard& rhs) = default;

        const std::string& dir() const { return _dir; }

        const packed_table_ptr table() const { return _table; }

        template <typename term_t>
        var_int_view_ptr term_view(const std::string& field) const {
            FieldToVarIntView::iterator result(_term_views.find(field));
            if (result == _term_views.end()) {
                const std::string filename(term_filename<term_t>(field));
                var_int_view_ptr  view(std::make_shared<MMappedVarIntView>(filename));
                result = _term_views.insert(std::make_pair(field, view)).first;
            }
            return result->second;
        }

        template <typename term_t>
        var_int_view_ptr docid_view(const std::string& field) const {
            FieldToVarIntView::iterator result(_docid_views.find(field));
            if (result == _docid_views.end()) {
                const std::string filename(docid_filename<term_t>(field));
                var_int_view_ptr  view(std::make_shared<MMappedVarIntView>(filename));
                std::cerr << "address: " << reinterpret_cast<const void *>(view->begin())
                          << " mapped: " << filename << std::endl;
                result = _docid_views.insert(std::make_pair(field, view)).first;
            }
            return result->second;
        }

        template <typename term_t>
        std::string term_filename(const std::string& field) const {
            return base_filename(field) + TermTraits<term_t>::term_file_extension();
        }

        template <typename term_t>
        std::string docid_filename(const std::string& field) const {
            return base_filename(field) + TermTraits<term_t>::docid_file_extension();
        }

        std::string name_of() const {
            const std::string::size_type pos(dir().find_last_of('/'));
            return pos == std::string::npos ? dir() : dir().substr(pos + 1);
        }

    private:
        std::string base_filename(const std::string& field) const {
            return dir() + "/fld-" + field + ".";
        }

        typedef std::map<std::string, var_int_view_ptr> FieldToVarIntView;
        mutable FieldToVarIntView _term_views;
        mutable FieldToVarIntView _docid_views;

        const std::string _dir;
        packed_table_ptr  _table;
    };

} // namespace imhotep

#endif
