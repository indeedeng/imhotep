#include <iostream>
#include <thread>

#include "merge_iterator.hpp"
#include "split_iterator.hpp"

using namespace std;
using namespace imhotep;

template <typename term_t>
void merge(const vector<string>& splits) {
    vector<split_iterator<term_t>> its;
    for (string split: splits) {
        its.push_back(split_iterator<term_t>(split));
    }

    merge_iterator<term_t> it(its.begin(), its.end());
    merge_iterator<term_t> end;
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
