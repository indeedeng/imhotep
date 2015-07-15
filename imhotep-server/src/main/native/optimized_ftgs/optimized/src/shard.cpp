#include "shard.hpp"

#include <sys/types.h>
#include <unistd.h>

#include <sstream>

namespace imhotep {

    Shard::Shard(const std::string&              dir,
                 const std::vector<std::string>& int_fields,
                 const std::vector<std::string>& str_fields,
                 packed_table_ptr                table)
        : _dir(dir)
        , _table(table) {
        /* !@# force caching of views for now... */
        typedef std::vector<std::string>::const_iterator It;
        for (It it(int_fields.begin()); it != int_fields.end(); ++it) {
            term_view<IntTerm>(*it);
            docid_view<IntTerm>(*it);
        }
        for (It it(str_fields.begin()); it != str_fields.end(); ++it) {
            term_view<StringTerm>(*it);
            docid_view<StringTerm>(*it);
        }
    }

    template <typename term_t>
    VarIntView Shard::term_view(const std::string& field) const {
        FieldToMMappedFile::iterator it(_term_views.find(field));
        if (it == _term_views.end()) {
            const std::string filename(term_filename<term_t>(field));
            std::shared_ptr<MMappedFile> view(std::make_shared<MMappedFile>(filename));
            it = _term_views.insert(std::make_pair(field, view)).first;
        }
        return VarIntView(it->second->begin(), it->second->end());
    }

    template <typename term_t>
    VarIntView Shard::docid_view(const std::string& field) const {
        FieldToMMappedFile::iterator it(_docid_views.find(field));
        if (it == _docid_views.end()) {
            const std::string filename(docid_filename<term_t>(field));
            std::shared_ptr<MMappedFile> view(std::make_shared<MMappedFile>(filename));
            it = _docid_views.insert(std::make_pair(field, view)).first;
        }
        return VarIntView(it->second->begin(), it->second->end());
    }

    SplitView Shard::split_view(const std::string& filename) const {
        const MMappedFile& file(*split_file(filename));
        return SplitView(file.begin(), file.end());
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
        os << splits_dir << '/' << name_of() << '.'
            //           << field << '.' << getpid() << '.' << split_num;
           << field << '.' << split_num;
        return os.str();
    }

    std::string Shard::name_of() const {
        const std::string::size_type pos(dir().find_last_of('/'));
        return pos == std::string::npos ? dir() : dir().substr(pos + 1);
    }

    std::string Shard::base_filename(const std::string& field) const {
        return dir() + "/fld-" + field + ".";
    }

    std::shared_ptr<MMappedFile> Shard::split_file(const std::string& filename) const {
        std::shared_ptr<MMappedFile> result;
        SplitFileMap::iterator it(_split_files.find(filename));
        if (it == _split_files.end()) {
            // !@# Consider having Shard explicitly delete these in dtor.
            result = std::make_shared<MMappedFile>(filename, true);
            _split_files[filename] = result;
        }
        else {
            result = it->second;
        }
        return result;
    }


    /* template instantiations */
    template VarIntView Shard::term_view<IntTerm>(const std::string& field) const;
    template VarIntView Shard::term_view<StringTerm>(const std::string& field) const;
    template VarIntView Shard::docid_view<IntTerm>(const std::string& field) const;
    template VarIntView Shard::docid_view<StringTerm>(const std::string& field) const;
    template std::string Shard::term_filename<IntTerm>(const std::string& field) const;
    template std::string Shard::term_filename<StringTerm>(const std::string& field) const;
    template std::string Shard::docid_filename<IntTerm>(const std::string& field) const;
    template std::string Shard::docid_filename<StringTerm>(const std::string& field) const;

} // namespace imhotep
