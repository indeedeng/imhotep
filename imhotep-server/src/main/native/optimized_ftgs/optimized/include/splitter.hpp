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

#include "shard.hpp"
#include "term_iterator.hpp"

namespace imhotep {

    template <typename term_t> struct SplitterTraits { typedef void               iterator_t; };
    template <> struct SplitterTraits<IntTerm>       { typedef IntTermIterator    iterator_t; };
    template <> struct SplitterTraits<StringTerm>    { typedef StringTermIterator iterator_t; };

    template <typename term_t>
    class Splitter {
    public:
        typedef typename SplitterTraits<term_t>::iterator_t term_iterator_t;

        Splitter(const Shard&       shard,
                 const std::string& field,
                 const std::string& output_dir,
                 size_t num_splits);

        Splitter(const Shard&       shard,
                 const std::string& field,
                 term_iterator_t    term_iterator,
                 const std::string& output_dir,
                 size_t num_splits);

        const Shard& shard() const { return _shard; }

        const std::vector<std::string>& splits() const { return _splits; }

        void run();

    private:
        FILE* open_split(const std::string& split);

        void encode(std::ostream& os, const term_t& term);

        const Shard       _shard;
        const std::string _field;
        term_iterator_t   _term_iterator;

        std::vector<std::string> _splits;
    };

    template <typename term_t>
    Splitter<term_t>::Splitter(const Shard&       shard,
                               const std::string& field,
                               const std::string& output_dir,
                               size_t             num_splits)
        : Splitter(shard, field,
                   term_iterator_t(shard.term_filename<term_t>(field)),
                   output_dir, num_splits)
    { }

    template <typename term_t>
    Splitter<term_t>::Splitter(const Shard&       shard,
                               const std::string& field,
                               term_iterator_t    term_iterator,
                               const std::string& output_dir,
                               size_t             num_splits)
        : _shard(shard)
        , _field(field)
        , _term_iterator(term_iterator)
    {
        for (size_t split(0); split < num_splits; ++split) {
            std::ostringstream os;
            os << output_dir << '/' << shard.name_of() << '.' << split;
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
        std::hash<typename term_t::id_t> hash_fun;
        term_iterator_t it(_term_iterator);
        term_iterator_t end;
        while (it != end) {
            const size_t   split(hash_fun(it->id()) % _splits.size());
            std::ofstream& of(*split_files[split]);
            const term_t&  term(*it);
            encode(of, term);
            ++it;
        }
        for (auto os_ptr: split_files) delete os_ptr;
    }

    template <typename term_t>
    FILE* Splitter<term_t>::open_split(const std::string& split)
    {
        FILE* result(fopen(split.c_str(), "w"));
        if (!result) {
            char message[1024];
            throw std::runtime_error(strerror_r(errno, message, sizeof(message)));
        }
        return result;
    }

} // namespace imhotep

#endif
