#include "gcc_preinclude.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <limits>

#include "shard.hpp"
#include "term_iterator.hpp"
#include "varintdecode.h"

using namespace std;
using namespace imhotep;

void dump_docids(const VarIntView docid_view, const IntTerm& term) {
    uint32_t count(term.doc_freq());
    if (count < 1) return;
    VarIntView view(docid_view.begin() + term.doc_offset(), docid_view.end());
    uint32_t docid(0);
    while (count > 0) {
        assert(!view.empty());
        const uint8_t  first(view.read());
        const uint32_t increment(view.read_varint<uint32_t>(first));
        docid += increment;
        cout << "   " << docid << endl;
        --count;
    }
}

void fancy_dump_docids(const VarIntView docid_view, const IntTerm& term) {
    VarIntView view(docid_view.begin() + term.doc_offset(), docid_view.end());
    const uint8_t* begin((uint8_t*) view.begin());
    int32_t count(term.doc_freq());
    uint32_t buffer[count];
    masked_vbyte_read_loop_delta(begin, buffer, count, 0);
    for (int32_t idx(0); idx < count; ++idx) {
        cout << "   " << buffer[idx] << endl;
    }
}


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
    // cout << " *it = " << *it << endl;
    // cout << "*end = " << *end << endl;
    // cout << "it == end? " << (it == end ? "yes" : "no") << endl;
    while (it != end) {
        cout << *it << endl;
        //        dump_docids(docid_view, *it);
        // cout << "----------------------------------------" << endl;
         fancy_dump_docids(docid_view, *it);
        ++it;
    }
}
