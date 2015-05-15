#ifndef SPLITTER_HPP
#define SPLITTER_HPP

#include <cstdint>
#include <cstring>
#include <limits>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "int_term_iterator.hpp"

namespace imhotep {

    /** !@# todo(johnf): Splitter either needs to produce doc
         addresses instead of offsets or somehow preserve doc base
         address (or file). */
    template <typename term_t>
    class Splitter {
        const std::string        _shard;
        const std::string        _field;
        std::vector<std::string> _splits;
    public:
        Splitter(const std::string& shard,
                 const std::string& field,
                 const std::string& output_dir,
                 size_t num_splits);

        const std::vector<std::string>& splits() const { return _splits; }
        void run();

    private:
        static constexpr uint64_t LARGE_PRIME_FOR_CLUSTER_SPLIT = 969168349;

        FILE* open_split(const std::string& split);
        size_t min_hash_int(int64_t term);
    };


    template <typename term_t>
    Splitter<term_t>::Splitter(const std::string& shard,
                               const std::string& field,
                               const std::string& output_dir,
                               size_t num_splits)
        : _shard(shard)
        , _field(field)
    {
        const std::string::size_type pos(shard.find_last_of('/'));
        const std::string file_name(pos == std::string::npos ?
                                    shard : shard.substr(pos + 1));
        for (size_t split(0); split < num_splits; ++split) {
            std::ostringstream os;
            os << output_dir << '/' << file_name << '.' << split;
            _splits.push_back(os.str());
        }
    }


    template <typename term_t>
    void Splitter<term_t>::run()
    {
        std::vector<std::ofstream*> split_files;
        for (std::string split: splits()) {
            split_files.push_back(new std::ofstream(split.c_str()));
        }
        int_term_iterator it(_shard, _field);
        int_term_iterator end;
        while (it != end) {
            const size_t   split(min_hash_int(it->id()));
            std::ofstream& of(*split_files[split]);
            const term_t term(*it);
            of.write(reinterpret_cast<const char *>(&term), sizeof(term_t));
            /*
            const int64_t  id(it->id());
            const int64_t  doc_offset(it->doc_offset());
            const int64_t  doc_freq(it->doc_freq());
            of.write(reinterpret_cast<const char*>(&id), sizeof(int64_t));
            of.write(reinterpret_cast<const char*>(&doc_offset), sizeof(int64_t));
            of.write(reinterpret_cast<const char*>(&doc_freq), sizeof(int64_t));
            */
            ++it;
        }
        for (auto os_ptr: split_files) delete os_ptr;
    }


    template <typename term_t>
    FILE* Splitter<term_t>::open_split(const std::string& split) {
        FILE* result(fopen(split.c_str(), "w"));
        if (!result) {
            char message[1024];
            throw std::runtime_error(strerror_r(errno, message, sizeof(message)));
        }
        return result;
    }


    template <typename term_t>
    size_t Splitter<term_t>::min_hash_int(int64_t term) {
        int32_t v(term * LARGE_PRIME_FOR_CLUSTER_SPLIT);
        v += 12345;
        v &= std::numeric_limits<int32_t>::max();
        v = v >> 16;
        return v % _splits.size();
    }

}

#endif
