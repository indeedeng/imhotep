#include <iostream>
#include <thread>

#include "shard.hpp"
#include "splitter.hpp"

using namespace std;
using namespace imhotep;

constexpr size_t num_splits = 8;

template <typename term_t>
void test_splitter(const vector<Shard>& shards,
                   const std::string& field,
                   const std::string& split_dir)
{
    vector<thread> threads;
    vector<Splitter<term_t>> splitters;
    for (Shard shard: shards) {
        splitters.push_back(Splitter<term_t>(shard, field, split_dir, num_splits));
    }
    for (Splitter<term_t>& splitter: splitters) {
        threads.push_back(thread([&splitter]() { splitter.run(); } ));
    }
    for (thread& th: threads) {
        th.join();
    }
}

int main(int argc, char *argv[])
{
    const string kind(argv[1]);
    const string field(argv[2]);
    const string split_dir(argv[3]);

    vector<Shard> shards;
    string str;
    while (getline(cin, str) && str.length()) {
        shards.push_back(Shard(str));
    }

    if (kind == "int") {
        test_splitter<IntTerm>(shards, field, split_dir);
    }
    else if (kind == "string") {
        test_splitter<StringTerm>(shards, field, split_dir);
    }
    else {
        cerr << "Say what?" << endl;
        exit(1);
    }
}
