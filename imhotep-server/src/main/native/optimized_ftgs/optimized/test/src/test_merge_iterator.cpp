#include <iostream>
#include <thread>

#include "merge_iterator.hpp"
#include "split_iterator.hpp"

using namespace std;
using namespace imhotep;

template <typename split_it, typename merge_it>
void merge(const vector<string>& splits) {
    vector<split_it> its;
    for (string split: splits) {
        its.push_back(split_it(split));
    }

    merge_it it(its.begin(), its.end());
    merge_it end;
    while (it != end) {
        cout << *it << endl;
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
        merge<split_int_term_iterator, merge_int_term_iterator>(splits);
    }
    else if (kind == "string") {
        merge<split_string_term_iterator, merge_string_term_iterator>(splits);
    }
    else {
        cerr << "Say what?" << endl;
        exit(1);
    }
}
