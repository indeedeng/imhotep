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
}

using namespace std;
using namespace imhotep;

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
                 const vector<string>& str_fields)
{
    vector<int64_t> mins;
    vector<int64_t> maxes;
    for (const string& field: int_fields) {
            std::pair<int64_t, int64_t> min_max(metadata.min_max<IntTerm>(field));
            mins.push_back(min_max.first);
            maxes.push_back(min_max.second);
    }
    for (const string& field: str_fields) {
        throw runtime_error("nyi: support for string fields");
    }

    const size_t num_cols(int_fields.size() + str_fields.size());

    TableMetadata table_meta(num_cols, mins.data(), maxes.data());

    Shard::packed_table_ptr table;
    table.reset(packed_table_create(metadata.num_docs(),
                                    mins.data(), maxes.data(),
                                    table_meta.sizes, table_meta.vec_nums, table_meta.offsets_in_vecs,
                                    vector<int8_t>(num_cols, 0).data(), num_cols));

    return Shard(metadata.shard().dir(), table);
}

void test_ftgs_runner(const vector<Shard>&  shards,
                      const vector<string>& int_fields,
                      const vector<string>& str_fields,
                      const string&         split_dir,
                      size_t                num_splits = 7)
{
    ExecutorService       executor;
    vector<ShardMetadata> metadata(shards.begin(), shards.end());
    vector<Shard> shards_with_tables;
    transform(metadata.begin(), metadata.end(), back_inserter(shards_with_tables),
              [&int_fields, &str_fields](const ShardMetadata& sm) {
                  return make_shard(sm, int_fields, str_fields);
              });

    FTGSRunner runner(shards_with_tables,
                      int_fields, str_fields, split_dir,
                      num_splits,
                      executor.num_workers(), // !@# fix wart in FTGSRunner ctor
                      executor);

    //    runner();
}

int main(int argc, char *argv[])
{
    namespace fs = boost::filesystem;
    namespace po = boost::program_options;

    const vector<string> empty;

    po::options_description desc("Allowed options");
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

    if (vm.count("help")) {
        cout << desc << endl;
        return 0;
    }

    vector<Shard> shards;
    fs::path shard_dir(vm["shard-dir"].as<string>());
    if (!fs::exists(shard_dir) || !fs::is_directory(shard_dir))
        throw runtime_error("no such shard-dir");
    transform(fs::directory_iterator(shard_dir), fs::directory_iterator(),
              back_inserter(shards),
              [](const fs::path& path) { return Shard(path.string()); });


    const vector<string> int_fields(vm["int-fields"].as<vector<string>>());
    const vector<string> str_fields(vm["str-fields"].as<vector<string>>());

    const string split_dir(vm["split-dir"].as<string>());
    const size_t num_splits(vm["num-splits"].as<size_t>());

    test_ftgs_runner(shards, int_fields, str_fields, split_dir, num_splits);

    exit(0);
}

