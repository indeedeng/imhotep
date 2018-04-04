#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#define restrict __restrict__
extern "C" {
#include "imhotep_native.h"
#include "table.h"
}

#include "mmapped_file.hpp"
#include "split_file_cache.hpp"
#include "split_view.hpp"
#include "term.hpp"
#include "term_index.hpp"
#include "var_int_view.hpp"

namespace imhotep {

    class Shard {
    public:
        /** A packed_table_ptr should be treated as a simple datum in the
            context of this class and users of it. I.e., these are created
            externally and their lifecycle managed elsewhere. (I'm not aware of
            a C++ pointer idiom appropriate for such a case or I'd use it.) */
        typedef ::packed_table_ptr packed_table_ptr;

        /** In Javaland, we maintain a cache of mmapped files, because according
            to JeffP, mmapping the same file multiple times is inefficient. This
            gets passed down to us through our JNI interface so that we can
            leverage it in the native code. This should not be confused with the
            per-field caches we maintain. */
        typedef std::unordered_map<std::string, void*> MapCache;

        Shard() : _table(0) { }

        explicit Shard(const std::string& dir,
                       packed_table_ptr   table     = packed_table_ptr(),
                       const MapCache&    map_cache = MapCache());

        Shard(const Shard& rhs) = delete;

        Shard& operator=(const Shard& rhs) = delete;

        const std::string& dir() const { return _dir; }

        packed_table_ptr table() const { return _table; }

        template <typename term_t>
        VarIntView term_view(const std::string& field) const;

        template <typename term_t>
        VarIntView docid_view(const std::string& field) const;

        IntTermIndex    int_term_index(const std::string& field) const;
        StringTermIndex str_term_index(const std::string& field) const;

        SplitView split_view(const std::string& filename) const;
        std::shared_ptr<SplitFile> split_file(const std::string& filename) const;

        template <typename term_t>
        std::string term_filename(const std::string& field) const;

        template <typename term_t>
        std::string docid_filename(const std::string& field) const;

        template <typename term_t>
        std::string index_filename(const std::string& field) const;

        std::string split_filename(const std::string& splits_dir,
                                   const std::string& field,
                                   size_t split_num) const;

        std::string name_of() const;

        std::string to_string() const;

    private:
        typedef std::map<std::string, std::shared_ptr<MMappedFile>> FieldToMMappedFile;

        std::string base_filename(const std::string& field) const;

        std::shared_ptr<MMappedFile>
        mmapped_file(const std::string&  field,
                     const std::string&  filename,
                     FieldToMMappedFile& cache) const;

        mutable FieldToMMappedFile _term_views;
        mutable FieldToMMappedFile _docid_views;
        mutable FieldToMMappedFile _int_term_indices;
        mutable FieldToMMappedFile _str_term_indices;
        mutable std::mutex         _mutex; // mutex for accessing FieldToMMappedFiles above.

        std::string      _dir;
        packed_table_ptr _table;
        MapCache         _map_cache;

        std::ostream& fieldmap_to_string(std::ostream& os, const FieldToMMappedFile& fieldAndMmappedFile) const;
        std::shared_ptr<SplitFileCache> _split_file_cache = std::make_shared<SplitFileCache>();
    };

} // namespace imhotep
