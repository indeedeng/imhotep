#include <iostream>
#include <thread>
#include <vector>
#include <utility>

#include "merge_iterator.hpp"
#include "split_iterator.hpp"

using namespace std;
using namespace imhotep;

template <typename term_t>
void merge(const vector<string>& splits) {
    Shard::packed_table_ptr table;
    vector<typename MergeIterator<term_t>::Entry> pairs;
    for (string split: splits) {
        pairs.push_back(make_pair(SplitIterator<term_t>(split), table));
    }

    MergeIterator<term_t> it(pairs.begin(), pairs.end());
    MergeIterator<term_t> end;
    while (it != end) {
        cout << (*it).first << endl;
        ++it;
    }
}

int main(int argc, char *argv[])
{
    const string kind(argv[1]);

    vector<string> splits;
    string str;
    while (getline(cin, str) && str.length()) {
        splits.push_back(str);
    }

    if (kind == "int") {
        merge<IntTerm>(splits);
    }
    else if (kind == "string") {
        merge<StringTerm>(splits);
    }
    else {
        cerr << "Say what?" << endl;
        exit(1);
    }
}
