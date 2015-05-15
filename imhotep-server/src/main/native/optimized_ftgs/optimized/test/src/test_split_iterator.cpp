#include <iostream>
#include <thread>

#include "merge_iterator.hpp"
#include "split_iterator.hpp"

using namespace std;
using namespace imhotep;

constexpr size_t num_splits = 8;

int main(int argc, char *argv[])
{
    vector<string> splits;
    string str;
    while (getline(cin, str) && str.length()) {
        splits.push_back(str);
    }

    vector<split_int_term_iterator> its;
    for (string split: splits) {
        its.push_back(split_int_term_iterator(split));
    }

    merge_int_term_iterator it(its.begin(), its.end());
    merge_int_term_iterator end;
    while (it != end) {
        cout << *it << endl;
        ++it;
    }
}
