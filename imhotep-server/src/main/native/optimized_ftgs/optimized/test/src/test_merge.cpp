#include <algorithm>
#include <chrono>
#include <iostream>
#include <limits>
#include <thread>

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>

#include "merger.hpp"
#include "string_term_id_pool.hpp"

using namespace std;
using namespace imhotep;

class Random
{
    boost::random::mt19937 _generator;
public:
    int operator()(int n) {
        boost::random::uniform_int_distribution<> dist(0, n);
        return dist(_generator);
    }
};

static Random rng;

string random_string(size_t length)
{
    auto randchar = []() -> char
        {
            const char charset[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
            const size_t max_index = (sizeof(charset) - 1);
            return charset[rng(numeric_limits<int>::max()) % max_index ];
        };
    string result(length, 0);
    generate_n(result.begin(), length, randchar);
    return result;
}

void check(StringTermIdPool& pool, const vector<pair<string, StringTermId>>& pairs) {
    for (auto pair: pairs) {
        if (pool.intern(pair.first) != pair.second) {
            cerr << "intern failure: " << pair.first << " " << pair.second << endl;
        }
        if (pool(pair.second) != pair.first) {
            cerr << "lookup failure: " << pair.first << " " << pair.second << endl;
        }
    }
}

void test_string_term_id_pool()
{
    StringTermIdPool pool;

    vector<pair<string, StringTermId>> pairs;
    //    for (size_t count(0); count < 1024 * 1024; ++count) {
    for (size_t count(0); count < 1024; ++count) {
        const string id(random_string(rng(128)));
        const StringTermId atom(pool.intern(id));
        pairs.push_back(make_pair(id, atom));
    }
    check(pool, pairs);

    for (auto pair: pairs) pool.intern(pair.first);
    check(pool, pairs);
}

typedef Merger<IntTerm> int_merger_t;

void producer(int_merger_t::InsertAndProcess& processor, size_t num_terms) {
    for (size_t count(0); count < num_terms; ++count) {
        processor(IntTerm(count, count, count));
        // this_thread::sleep_for(chrono::milliseconds(rng(100)));
    }
}

int main(int argc, char *argv[])
{
    test_string_term_id_pool();

    int_merger_t merger;
    int_merger_t::Consume consumer([](const IntTerm& term) {
            //            cout << this_thread::get_id() << ": " << term << endl;
        });

    const size_t num_splits = max(thread::hardware_concurrency(), unsigned(8));
    cerr << "num_splits: " << num_splits << endl;
    const size_t terms_per_split = 8000000 / num_splits;

    vector<int_merger_t::InsertAndProcess> processors;
    for (size_t count(0); count < num_splits; ++count) {
        processors.push_back(merger.attach(consumer));
    }
    vector<thread> producers;
    for (int_merger_t::InsertAndProcess& processor: processors) {
        producers.push_back(thread([&processor, terms_per_split]() {
                    producer(processor, terms_per_split);
                }));
    }
    for (thread& producer: producers) {
        producer.join();
    }
    merger.join(consumer);
}
