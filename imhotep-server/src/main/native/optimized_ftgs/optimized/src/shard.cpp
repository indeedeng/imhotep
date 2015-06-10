#include "shard.hpp"

namespace imhotep {

    Shard::Shard(const std::string& dir, packed_table_ptr   table)
        : _dir(dir)
        , _table(table)
    { }

    template <typename term_t>
    Shard::var_int_view_ptr Shard::term_view(const std::string& field) const {
        FieldToVarIntView::iterator result(_term_views.find(field));
        if (result == _term_views.end()) {
            const std::string filename(term_filename<term_t>(field));
            var_int_view_ptr  view(std::make_shared<MMappedVarIntView>(filename));
            result = _term_views.insert(std::make_pair(field, view)).first;
        }
        return result->second;
    }

    template <typename term_t>
    Shard::var_int_view_ptr Shard::docid_view(const std::string& field) const {
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
    std::string Shard::term_filename(const std::string& field) const {
        return base_filename(field) + TermTraits<term_t>::term_file_extension();
    }

    template <typename term_t>
    std::string Shard::docid_filename(const std::string& field) const {
        return base_filename(field) + TermTraits<term_t>::docid_file_extension();
    }

    std::string Shard::split_filename(const std::string& splits_dir,
                                      const std::string& field,
                                      size_t split_num) const {
        std::ostringstream os;
        os << splits_dir << '/' << name_of() << '.' << field << '.' << split_num;
        return os.str();
    }

    std::string Shard::name_of() const {
        const std::string::size_type pos(dir().find_last_of('/'));
        return pos == std::string::npos ? dir() : dir().substr(pos + 1);
    }

    std::string Shard::base_filename(const std::string& field) const {
        return dir() + "/fld-" + field + ".";
    }

    /* template instantiations */
    template Shard::var_int_view_ptr Shard::term_view<IntTerm>(const std::string& field) const;
    template Shard::var_int_view_ptr Shard::term_view<StringTerm>(const std::string& field) const;
    template Shard::var_int_view_ptr Shard::docid_view<IntTerm>(const std::string& field) const;
    template Shard::var_int_view_ptr Shard::docid_view<StringTerm>(const std::string& field) const;
    template std::string Shard::term_filename<IntTerm>(const std::string& field) const;
    template std::string Shard::term_filename<StringTerm>(const std::string& field) const;
    template std::string Shard::docid_filename<IntTerm>(const std::string& field) const;
    template std::string Shard::docid_filename<StringTerm>(const std::string& field) const;



} // namespace imhotep
