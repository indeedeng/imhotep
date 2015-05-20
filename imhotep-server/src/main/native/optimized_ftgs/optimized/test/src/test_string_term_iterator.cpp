#include <algorithm>
#include <iostream>
#include <limits>

#include "shard.hpp"
#include "term_iterator.hpp"

using namespace std;
using namespace imhotep;

int main(int argc, char *argv[])
{
    if (argc != 3) {
        cerr << "usage: " << argv[0] << " <shard dir> <field name>" << endl;
    }

    const string shard_dir(argv[1]);
    const string field_name(argv[2]);

    StringTermIterator it(Shard::term_filename<StringTerm>(shard_dir, field_name));
    StringTermIterator end;
    while (it != end) {
        cout << *it << endl;
        ++it;
    }
}
