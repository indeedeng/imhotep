#include "shard.hpp"

#include <sys/types.h>
#include <unistd.h>

#include <sstream>

#include <iostream>             // !@# debugging...

namespace imhotep {

    Shard::Shard(const std::string&              dir,
                 const std::vector<std::string>& int_fields,
                 const std::vector<std::string>& str_fields,
                 packed_table_ptr                table,
                 const MapCache&                 map_cache)
        : _dir(dir)
        , _table(table)
        , _map_cache(map_cache)
    {
        /* Note to self: during development, we forced caching of
           views as an optimization, but that shouldn't be necessary
           given the advent of MapCache. If performance degrades we
           should reexamine. */
    }

    std::shared_ptr<MMappedFile>
    Shard::mmapped_file(const std::string&  field,
                        const std::string&  filename,
                        FieldToMMappedFile& cache) const {
        FieldToMMappedFile::iterator it(cache.find(field));
        if (it == cache.end()) {
            const MapCache::const_iterator mit(_map_cache.find(filename));
            std::shared_ptr<MMappedFile> entry
                (mit != _map_cache.end() ?
                 std::make_shared<MMappedFile>(filename, mit->second) :
                 std::make_shared<MMappedFile>(filename));
            it = cache.insert(std::make_pair(field, entry)).first;
        }
        return it->second;
    }

    template <typename term_t>
    VarIntView Shard::term_view(const std::string& field) const {
        const std::string filename(term_filename<term_t>(field));
        std::shared_ptr<MMappedFile> mmapped(mmapped_file(field, filename, _term_views));
        return VarIntView(mmapped->begin(), mmapped->end());
    }

    template <typename term_t>
    VarIntView Shard::docid_view(const std::string& field) const {
        const std::string filename(docid_filename<term_t>(field));
        std::shared_ptr<MMappedFile> mmapped(mmapped_file(field, filename, _docid_views));
        return VarIntView(mmapped->begin(), mmapped->end());
    }

    IntTermIndex Shard::int_term_index(const std::string& field) const {
        const std::string filename(index_filename<IntTerm>(field));
        std::shared_ptr<MMappedFile> mmapped(mmapped_file(field, filename, _int_term_indices));
        return IntTermIndex(mmapped->begin(), mmapped->end(), term_view<IntTerm>(field));
    }

    StringTermIndex Shard::str_term_index(const std::string& field) const {
        const std::string filename(index_filename<StringTerm>(field));
        std::shared_ptr<MMappedFile> mmapped(mmapped_file(field, filename, _str_term_indices));
        return StringTermIndex(mmapped->begin(), mmapped->end(), term_view<IntTerm>(field));
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

    template <typename term_t>
    std::string Shard::index_filename(const std::string& field) const {
        return base_filename(field) + TermTraits<term_t>::index_file_extension();
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
            result = std::make_shared<MMappedFile>(filename, true, true);
            _split_files[filename] = result;
        }
        else {
            result = it->second;
        }
        return result;
    }

    std::string Shard::to_string() const {
        std::ostringstream os;
        os << "{ name_of: " << name_of()
           << ", dir: "     << dir()
           << ", table: "   << table()
           << ", map_cache: [";
        bool first_entry = true;
        for (auto entry: _map_cache) {
            if (!first_entry) {
                os << ", ";
                first_entry = false;
            }
            os << "{ name: "  << entry.first
               << ", value: " << reinterpret_cast<long>(entry.second)
               << " }";
        }
        os << "] }";
        return os.str();
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
