#include <iostream>
#include <thread>

#include "splitter.hpp"

using namespace std;
using namespace imhotep;

constexpr size_t num_splits = 8;

int main(int argc, char *argv[])
{
    const string field(argv[1]);
    const string split_dir(argv[2]);

    vector<string> shards;
    string str;
    while (getline(cin, str) && str.length()) {
        shards.push_back(str);
    }

    vector<Splitter<IntTerm>> splitters;
    for (string shard: shards) {
        splitters.push_back(Splitter<IntTerm>(shard, field, split_dir, num_splits));
    }
    vector<thread> threads;
    for (Splitter<IntTerm>& splitter: splitters) {
        threads.push_back(thread([&splitter]() { splitter.run(); } ));
    }
    for (thread& th: threads) {
        th.join();
    }
}
