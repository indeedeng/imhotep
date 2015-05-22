#ifndef SHARD_HPP
#define SHARD_HPP

#include <memory>
#include <string>

#define restrict __restrict__
extern "C" {
#include "imhotep_native.h"
}
#include "term.hpp"

namespace imhotep {

    class Shard {
    public:
        typedef std::shared_ptr<packed_table_t> packed_table_ptr;

        explicit Shard(const std::string& dir,
              packed_table_ptr   table = packed_table_ptr())
            : _dir(dir)
            , _table(table)
        { }

        Shard(const Shard& rhs) = default;

        Shard& operator=(const Shard& rhs) = default;

        const std::string& dir() const { return _dir; }

        packed_table_ptr table() { return _table; }

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

        const std::string _dir;
        packed_table_ptr  _table;
    };

} // namespace imhotep

#endif
