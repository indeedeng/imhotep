#include <iostream>
#include <thread>
#include <utility>

#include "executor_service.hpp"
#include "shard.hpp"
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
        sources.push_back(make_pair(Shard::name_of(shard), it));
    }

    TermProvider<term_t> provider(sources, field, split_dir, num_splits);

    ExecutorService es;
    for (size_t split_num(0); split_num < num_splits; ++split_num) {
        es.enqueue([&provider, split_num] {
                TermDescIterator<MergeIterator<term_t>> it(provider.merge(split_num));
                TermDescIterator<MergeIterator<term_t>> end;
                size_t num_descs(0);
                while (it != end) {
                    ++num_descs;
                    ++it;
                }
                cout << "num_descs: " << num_descs << endl;
            });
    }
    es.awaitCompletion();
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
