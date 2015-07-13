#include <algorithm>
#include <iostream>
#include <limits>
#include <thread>
#include <utility>
#include <vector>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <yaml-cpp/yaml.h>

#include "executor_service.hpp"
#include "ftgs_runner.hpp"
#include "shard.hpp"
#include "term_iterator.hpp"

#define restrict __restrict__
extern "C" {
#include "imhotep_native.h"
#include "test_patch.h"
#include "varintdecode.h"
}

using namespace std;
using namespace imhotep;

namespace test_ftgs_namespace{
    class ShardMetadata {
        const Shard&     _shard;
        const YAML::Node _metadata;
    public:
        ShardMetadata(const Shard& shard)
            : _shard(shard)
            , _metadata(YAML::LoadFile(shard.dir() + "/metadata.txt"))
        { }

        const Shard& shard() const { return _shard; }

        size_t num_docs() const { return _metadata["numDocs"].as<size_t>(); }

        template <typename term_t>
        pair<int64_t, int64_t> min_max(const std::string& field) const;
    };

    template<>
    pair<int64_t, int64_t> ShardMetadata::min_max<IntTerm>(const std::string& field) const
    {
        pair<int64_t, int64_t> result(numeric_limits<int64_t>::max(), numeric_limits<int64_t>::min());
        IntTermIterator it(_shard, field);
        IntTermIterator end;
        while (it != end) {
            result.first  = min(it->id(), result.first);
            result.second = max(it->id(), result.second);
            ++it;
        }
        return result;
    }

    template<>
    pair<int64_t, int64_t> ShardMetadata::min_max<StringTerm>(const std::string& field) const
    {
        return make_pair(0, 0);
    }

    struct TableMetadata {
        int32_t* sizes           = 0;
        int32_t* vec_nums        = 0;
        int32_t* offsets_in_vecs = 0;

        TableMetadata(int num_cols,
                      const int64_t * restrict mins,
                      const int64_t * restrict maxes)
            : sizes(get_sizes(num_cols, mins, maxes))
            , vec_nums(get_vec_nums(num_cols, mins, maxes, sizes))
            , offsets_in_vecs(get_offsets_in_vecs(num_cols, mins, maxes, sizes))
        { }

        TableMetadata(const TableMetadata& rhs) = delete;

        ~TableMetadata() {
            free(sizes);
            free(vec_nums);
            free(offsets_in_vecs);
        }
    };

    Shard make_shard(const ShardMetadata&  metadata,
                     const vector<string>& int_fields,
                     const vector<string>& str_fields,
                     const bool            only_binary_metrics)
    {
        vector<int64_t> mins;
        vector<int64_t> maxes;
        for (const string& field: int_fields) {
            std::pair<int64_t, int64_t> min_max(metadata.min_max<IntTerm>(field));
            mins.push_back(min_max.first);
            maxes.push_back(min_max.second);
        }

        const size_t num_cols(int_fields.size() + str_fields.size());

        TableMetadata table_meta(num_cols, mins.data(), maxes.data());

        Shard::packed_table_ptr table;

        table = packed_table_create(metadata.num_docs(),
                                    mins.data(), maxes.data(),
                                    table_meta.sizes, table_meta.vec_nums, table_meta.offsets_in_vecs,
                                    vector<int8_t>(num_cols, 0).data(), num_cols, only_binary_metrics);
        return Shard(metadata.shard().dir(), int_fields, str_fields, table);
    }



    vector<Shard> create_shards_with_tables(const vector<Shard>&  shards,
                                            const vector<string>& int_fields,
                                            const vector<string>& str_fields,
                                            const bool            only_binary_metrics) {
        vector<ShardMetadata> metadata(shards.begin(), shards.end());
        vector<Shard> shards_with_tables;
        transform(metadata.begin(), metadata.end(), back_inserter(shards_with_tables),
                  [&int_fields, &str_fields, only_binary_metrics](const ShardMetadata& sm) {
                      return make_shard(sm, int_fields, str_fields, only_binary_metrics);
                  });
        return shards_with_tables;
    }



    void test_ftgs_runner(const vector<Shard>&  shards,
                          const vector<string>& int_fields,
                          const vector<string>& str_fields,
                          const string&         splits_dir,
                          size_t                num_splits,
                          const int             num_groups,
                          const int             num_metrics,
                          std::vector<int> &    socket_fds,
                          const bool            only_binary_metrics=false){
        vector<Shard> shards_with_tables(create_shards_with_tables(shards, int_fields, str_fields,
                                                                   only_binary_metrics));
        ExecutorService       executor;
        const size_t num_workers= executor.num_workers();

        FTGSRunner runner(shards_with_tables,
                          int_fields,
                          str_fields,
                          splits_dir,
                          num_splits,
                          num_workers,
                          executor);

        runner.run(num_groups,
                   num_metrics,
                   only_binary_metrics,
                   shards_with_tables[0].table(),
                   socket_fds);

        for (Shard shard: shards_with_tables) {
            packed_table_destroy(const_cast<packed_table_t*>(shard.table()));
        }
    }
}

// TODO calculate int_fields,stringFields, numGroups, numStats,
// num_metrics and sockets based on the min info needed.  base yourself
// on the java code
int main(int argc, char *argv[])
{
    simdvbyteinit();

    namespace fs = boost::filesystem;
    namespace po = boost::program_options;

    const vector<string> empty;

    po::options_description desc("At the moment there is no support for string fields.\n\
                                 Allowed options");
    desc.add_options()
        ("help", "simple test for ftgs runner")
        ("shard-dir",  po::value<string>(),
         "root of shards directory")
        ("int-fields", po::value<vector<string>>()->default_value(empty, "FIELDS")->multitoken(),
         "list of int fields")
        ("str-fields", po::value<vector<string>>()->default_value(empty, "FIELDS")->multitoken(),
         "list of string fields")
        ("split-dir",  po::value<string>()->default_value("/tmp/splits", "DIR"),
         "directory to stash temp split files")
        ("num-splits", po::value<size_t>()->default_value(13, "UINT"),
         "number of splits to use")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help") || (!vm.count("shard-dir") || !vm.count("int-fields"))) {
        cerr << desc << endl;
        return 1;
    }

    const vector<string> int_fields(vm["int-fields"].as<vector<string>>());
    const vector<string> str_fields(vm["str-fields"].as<vector<string>>());

    vector<Shard> shards;
    fs::path shard_dir(vm["shard-dir"].as<string>());
    if (!fs::exists(shard_dir) || !fs::is_directory(shard_dir))
        throw runtime_error("no such shard-dir");
    transform(fs::directory_iterator(shard_dir), fs::directory_iterator(),
              back_inserter(shards),
              [&int_fields, &str_fields](const fs::path& path) {
                  return Shard(path.string(), int_fields, str_fields);
              });


    const string split_dir(vm["split-dir"].as<string>());
    const size_t num_splits(vm["num-splits"].as<size_t>());
    const size_t num_groups =1;
    const size_t num_metrics=int_fields.size();
    std::vector<int> sockets(num_splits, 1);
    bool only_binary_metrics=false;
    test_ftgs_namespace::test_ftgs_runner(shards, int_fields, str_fields,
                                          split_dir, num_splits,
                                          num_groups, num_metrics,
                                          sockets, only_binary_metrics);
}

