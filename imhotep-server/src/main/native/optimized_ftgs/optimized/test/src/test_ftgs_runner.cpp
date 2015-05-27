#include <algorithm>
#include <iostream>
#include <thread>
#include <vector>
#include <utility>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include "executor_service.hpp"
#include "ftgs_runner.hpp"
#include "shard.hpp"

using namespace std;
using namespace imhotep;

void test_ftgs_runner(const vector<Shard>&  shards,
                      const vector<string>& int_fields,
                      const vector<string>& str_fields,
                      const string&         split_dir,
                      size_t                num_splits = 7) {

    ExecutorService executor;
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
        ("num-splits", po::value<int>()->default_value(13, "UINT"),
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

