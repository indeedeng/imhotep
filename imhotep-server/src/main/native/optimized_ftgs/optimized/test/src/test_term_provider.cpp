#include <iostream>
#include <thread>
#include <utility>

#include "term_provider.hpp"

using namespace std;
using namespace imhotep;

template <typename term_t>
void test_term_provider(const vector<string>& shards,
                        const string& field,
                        const string& split_dir,
                        size_t num_splits=7) {

    vector<typename TermProvider<term_t>::term_source_t> sources;
    for (string shard: shards) {
        TermIterator<term_t> it(Shard::term_filename<term_t>(shard, field));
        sources.push_back(make_pair(shard, it));
    }

    TermProvider<term_t> provider(sources, field, split_dir, num_splits);
    for (auto split: provider.splits()) {
        cout << split.first << ':' + split.second << endl;
    }
}

int main(int argc, char *argv[])
{
    const string kind(argv[1]);
    const string field(argv[2]);
    const string split_dir(argv[3]);

    vector<string> shards;
    string str;
    while (getline(cin, str) && str.length()) {
        shards.push_back(str);
    }

    if (kind == "int") {
        test_term_provider<IntTerm>(shards, field, split_dir);
    }
    else if (kind == "string") {
        test_term_provider<StringTerm>(shards, field, split_dir);
    }
    else {
        cerr << "Say what?" << endl;
        exit(1);
    }
}
