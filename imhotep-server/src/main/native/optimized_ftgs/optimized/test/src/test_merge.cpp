#include <algorithm>
#include <chrono>
#include <iostream>
#include <limits>
#include <thread>

#include "int_term_iterator.hpp"
#include "merger.hpp"

using namespace std;
using namespace imhotep;

typedef Merger<IntTerm> int_merger_t;

constexpr long LARGE_PRIME_FOR_CLUSTER_SPLIT = 969168349;
constexpr size_t num_splits = 8;

inline
int min_hash_int(long term) {
    int v;
    v = (int) (term * LARGE_PRIME_FOR_CLUSTER_SPLIT);
    v += 12345;
    v &= numeric_limits<int32_t>::max();
    v = v >> 16;
    return v % num_splits;
}

void producer(vector<int_merger_t::InsertAndProcess>& processors,
              const string& shard,
              const string& field)
{
    int_term_iterator it(shard, field);
    int_term_iterator end;
    while (it != end) {
        processors[min_hash_int(it->id())](*it);
        ++it;
    }
}

int main(int argc, char *argv[])
{
    const string field(argv[1]);

    vector<string> shards;
    string str;
    while (getline(cin, str) && str.length()) {
        shards.push_back(str);
    }

    size_t total_terms(0);

    int_merger_t merger;
    int_merger_t::Consume consumer =
        [&total_terms](const IntTerm& term) {
        ++total_terms;
    };

    vector<int_merger_t::InsertAndProcess> processors;
    for (size_t split(0); split < num_splits; ++split) {
        processors.push_back(merger.attach(consumer));
    }

    vector<thread> producers;

    for (size_t i(0); i < shards.size(); ++i) {
        const string& shard(shards[i]);
        producers.push_back(thread([&processors, shard, field]() {
                    producer(processors, shard, field);
                }));
    }

    for (thread& producer: producers) {
        producer.join();
    }
    merger.join(consumer);

    cout << "total terms: " << total_terms << endl;
}
