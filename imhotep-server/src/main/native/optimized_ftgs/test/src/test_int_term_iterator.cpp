#include "gcc_preinclude.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <limits>

#include "docid_iterator.hpp"
#include "shard.hpp"
#include "term_iterator.hpp"
#include "varintdecode.h"

using namespace std;
using namespace imhotep;

int main(int argc, char *argv[])
{
    if (argc != 3) {
        cerr << "usage: " << argv[0] << " <shard dir> <field name>" << endl;
    }

    simdvbyteinit();

    std::vector<std::string> int_terms;
    std::vector<std::string> str_terms;

    const string field_name(argv[2]);
    int_terms.push_back(field_name);
    const Shard shard(argv[1], int_terms, str_terms);

    const VarIntView docid_view(shard.docid_view<IntTerm>(field_name));

    IntTermIterator it(shard.term_view<IntTerm>(field_name));
    IntTermIterator end;
    while (it != end) {
        cout << *it << endl;
        DocIdIterator dit(docid_view, it->doc_offset(), it->doc_freq());
        DocIdIterator dend;
        while (dit != dend) {
            cout << "   " << *dit << endl;
            ++dit;
        }
        ++it;
    }
}
