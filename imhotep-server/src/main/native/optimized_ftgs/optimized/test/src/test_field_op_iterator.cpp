#include <algorithm>
#include <iostream>
#include <limits>
#include <thread>
#include <utility>
#include <vector>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include "executor_service.hpp"
#include "field_op_iterator.hpp"
#include "shard.hpp"
#include "term_providers.hpp"

using namespace std;
using namespace imhotep;

int main(int argc, char *argv[])
{
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
        ("split-dir",  po::value<string>()->default_value("/tmp/splits", "DIR"),
         "directory to stash temp split files")
        ("num-splits", po::value<size_t>()->default_value(13, "UINT"),
         "number of splits to use")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help") ||
        (!vm.count("shard-dir") || !vm.count("int-fields")))
    {
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
    const string         split_dir(vm["split-dir"].as<string>());
    const size_t         num_splits(vm["num-splits"].as<size_t>());

    ExecutorService executor;
    TermProviders<IntTerm> providers(shards, int_fields, split_dir, num_splits, executor);

    for (size_t split_num(0); split_num < num_splits; ++split_num) {
        std::mutex cout_mutex;
        executor.enqueue([&providers, split_num, &cout_mutex] {
                size_t                   num_ops(0);
                FieldOpIterator<IntTerm> it(providers, split_num);
                FieldOpIterator<IntTerm> end;
                while (it != end) {
                    {
                        std::lock_guard<std::mutex> guard(cout_mutex);
                        cout << it->to_string() << endl;
                    }
                    ++num_ops;
                    ++it;
                }
                std::lock_guard<std::mutex> guard(cout_mutex);
                cout << "split_num: " << split_num << " num_ops: " << num_ops << endl;
            });
    }
    executor.await_completion();
}

