#ifndef SHARD_HPP
#define SHARD_HPP

#include <string>

#include "term.hpp"

namespace imhotep {

    class Shard {
    public:
        template <typename term_t>
        static std::string term_filename(const std::string& shard_dir,
                                         const std::string& field) {
            return base_filename(shard_dir, field) +
                term_traits<term_t>::term_file_extension();
        }

        template <typename term_t>
        static std::string docid_filename(const std::string& shard_dir,
                                          const std::string& field) {
            return base_filename(shard_dir, field) +
                term_traits<term_t>::docid_file_extension();
        }

    private:
        static std::string base_filename(const std::string& shard_dir,
                                         const std::string& field) {
            return shard_dir + "/fld-" + field + ".";
        }
    };

} // namespace imhotep

#endif
