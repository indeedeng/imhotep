#include <algorithm>
#include <iostream>
#include <thread>

#include "shard.hpp"
#include "splitter.hpp"

using namespace std;
using namespace imhotep;

template <typename term_t>
void test_splitter(const vector<Shard>& shards,
                   const std::string& field,
                   const std::string& split_dir,
                   size_t num_splits)
{
    vector<thread> threads;
    vector<Splitter<term_t>> splitters;

    for (vector<Shard>::const_iterator it(shards.begin()); it != shards.end(); ++it) {
        const Shard& shard(*it);
        splitters.push_back(Splitter<term_t>(shard, field, split_dir, num_splits));
    }

    for (typename vector<Splitter<term_t>>::iterator it(splitters.begin());
         it != splitters.end(); ++it) {
        Splitter<term_t>& splitter(*it);
        threads.push_back(std::bind(&Splitter<term_t>::run, splitter));
    }
    for (vector<thread>::iterator it(threads.begin()); it != threads.end(); ++it) {
        it->join();
    }
}

int main(int argc, char *argv[])
{
    const string kind(argv[1]);
    const string field(argv[2]);
    const string split_dir(argv[3]);
    const size_t num_splits(atoi(argv[4]));

    std::vector<std::string> int_terms;
    std::vector<std::string> str_terms;

    std::vector<std::string> shard_names;
    std::string str;
    while (getline(cin, str) && str.length()) {
        shard_names.push_back(str);
    }

    vector<Shard> shards;

    if (kind == "int") {
        int_terms.push_back(field);
        for (vector<string>::const_iterator it(shard_names.begin());
             it != shard_names.end(); ++it) {
            shards.emplace_back(Shard(*it, int_terms, str_terms));
        }
        test_splitter<IntTerm>(shards, field, split_dir, num_splits);
    }
    else if (kind == "string") {
        str_terms.push_back(field);
        for (vector<string>::const_iterator it(shard_names.begin());
             it != shard_names.end(); ++it) {
            shards.emplace_back(Shard(*it, int_terms, str_terms));
        }
        test_splitter<StringTerm>(shards, field, split_dir, num_splits);
    }
    else {
        cerr << "Say what?" << endl;
        exit(1);
    }
}
