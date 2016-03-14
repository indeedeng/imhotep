#include "gcc_preinclude.h"

#include <iostream>

#include "field.hpp"
#include "shard.hpp"
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

    const VarIntView term_view(shard.term_view<IntTerm>(field_name));
    const VarIntView docid_view(shard.docid_view<IntTerm>(field_name));

    const Field<IntTerm> field(term_view, docid_view);
    for (size_t idx(0); idx < field().size(); ++idx) {
        cout << idx << " : " << field()[idx] << endl;
    }
    cout << "min: " << field.min() << endl;
    cout << "max: " << field.max() << endl;
}
