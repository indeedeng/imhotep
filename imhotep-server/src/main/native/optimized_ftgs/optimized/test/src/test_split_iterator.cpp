#include <iostream>
#include <thread>

#include "split_iterator.hpp"

using namespace std;
using namespace imhotep;

constexpr size_t num_splits = 8;

int main(int argc, char *argv[])
{
    const string split(argv[1]);

    split_int_term_iterator it(split);
    const split_int_term_iterator end;
    while (it != end) {
        cout << *it << endl;
        ++it;
    }
}
