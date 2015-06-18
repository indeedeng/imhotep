#ifndef SPLITTER_HPP
#define SPLITTER_HPP

#include <cstdint>
#include <cstring>
#include <fstream>
#include <limits>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>

#include "shard.hpp"
#include "term_iterator.hpp"

namespace imhotep {

    template <typename term_t> struct SplitterTraits { typedef void               iterator_t; };
    template <> struct SplitterTraits<IntTerm>       { typedef IntTermIterator    iterator_t; };
    template <> struct SplitterTraits<StringTerm>    { typedef StringTermIterator iterator_t; };

    template <typename term_t>
    class Splitter {
    public:
        typedef std::map<size_t, std::string> SplitNumToField;

        typedef typename SplitterTraits<term_t>::iterator_t term_iterator_t;

        Splitter(const Shard&       shard,
                 const std::string& field,
                 const std::string& splits_dir,
                 size_t num_splits);

        Splitter(const Shard&       shard,
                 const std::string& field,
                 term_iterator_t    term_iterator,
                 const std::string& splits_dir,
                 size_t num_splits);

        const Shard& shard() const { return _shard; }

        const std::string& splits_dir() const { return _splits_dir; }

        const SplitNumToField& splits() const { return _splits; } // !@# fix this name!

        void run();

    private:
        void encode(std::ostream& os, const term_t& term);

        const Shard       _shard;
        const std::string _splits_dir;
        const std::string _field;
        term_iterator_t   _term_iterator;

        SplitNumToField _splits;
    };

    template <typename term_t>
    Splitter<term_t>::Splitter(const Shard&       shard,
                               const std::string& field,
                               const std::string& splits_dir,
                               size_t             num_splits)
        : Splitter(shard, field,
                   term_iterator_t(shard, field),
                   splits_dir, num_splits)
    { }

    template <typename term_t>
    Splitter<term_t>::Splitter(const Shard&       shard,
                               const std::string& field,
                               term_iterator_t    term_iterator,
                               const std::string& splits_dir,
                               size_t             num_splits)
        : _shard(shard)
        , _splits_dir(splits_dir)
        , _field(field)
        , _term_iterator(term_iterator)
    {
        for (size_t split_num(0); split_num < num_splits; ++split_num) {
            _splits[split_num] = field;
        }
    }

    template <typename term_t>
    void Splitter<term_t>::run()
    {
        std::vector<std::ofstream*> split_files;
        for (auto kv: splits()) {
            const std::string filename(_shard.split_filename(splits_dir(), kv.second, kv.first));
            split_files.push_back(new std::ofstream(filename.c_str(),
                                                    std::ios::binary | std::ios::out | std::ios::trunc));
        }

        term_iterator_t it(_term_iterator);
        term_iterator_t end;
        while (it != end) {
            const size_t hash_val(it->hash());
            const size_t split(hash_val % _splits.size());

            // std::cerr << __FUNCTION__
            //           << " id: " << it->id()
            //           << " hash_val: " << hash_val
            //           << " split: " << split
            //           << std::endl;

            std::ofstream& of(*split_files[split]);
            const term_t&  term(*it);
            encode(of, term);
            ++it;
        }

        for (auto os_ptr: split_files) delete os_ptr;
    }

} // namespace imhotep

#endif
