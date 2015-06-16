#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <utility>

#include "mmapped_file.hpp"
#include "split_iterator.hpp"

using namespace std;
using namespace imhotep;

template <typename term_t>
void test_it(const vector<string>& splits) {
    for (string split: splits) {
        MMappedFile file(split);
        SplitView view(file.begin(), file.end());
        SplitIterator<term_t> split_it(view);
        SplitIterator<term_t> end;
        while (split_it != end) {
            cout << *split_it << endl;
            ++split_it;
        }
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
        test_it<IntTerm>(splits);
    }
    else if (kind == "string") {
        test_it<StringTerm>(splits);
    }
    else {
        cerr << "Say what?" << endl;
        exit(1);
    }
}
