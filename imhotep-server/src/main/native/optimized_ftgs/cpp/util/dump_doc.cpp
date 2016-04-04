#include "docid_iterator.hpp"
#include "shard.hpp"
#include "shard_metadata.hpp"

#include <boost/program_options.hpp>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <vector>

#define restrict __restrict__
extern "C" {
#include "varintdecode.h"
}

using namespace imhotep;
using namespace std;

template <typename term_t>
bool has_doc(VarIntView docid_view, TermIterator<term_t> it, int64_t docid)
{
    DocIdIterator docid_it(docid_view, it->doc_offset(), it->doc_freq());
    DocIdIterator docid_end;
    while (docid_it != docid_end && *docid_it != docid) {
        ++docid_it;
    }
    return docid_it != docid_end;
}

template <typename term_t>
void dump_field(Shard& shard, const string& field, int64_t docid)
{
    VarIntView           docid_view(shard.docid_view<term_t>(field));
    TermIterator<term_t> it(shard.term_view<term_t>(field));
    TermIterator<term_t> end;

    cout << setw(32) << endl << field << " :";

    vector<id_t> terms;
    while (it != end) {
        const bool found(has_doc(docid_view, it, docid));
        if (found) {
            cout << " " << (*it).id();
        }
         ++it;
    }
}

int main(int argc, char* argv[])
{
    simdvbyteinit();

    namespace po = boost::program_options;

    const vector<string> empty;

    po::options_description desc("TermIndex tests");
    desc.add_options()
        ("help", "TermIndex tests")
        ("docid", po::value<int64_t>(), "field to dump")
        ("shard", po::value<string>(),  "shard directory")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help") || !vm.count("shard")) {
        cerr << desc << endl;
        return 0;
    }

    const string        dir(vm["shard"].as<string>());
    const int64_t       docid(vm["docid"].as<int64_t>());
    const Shard         proto_shard(dir, empty, empty);
    const ShardMetadata smd(proto_shard);

    auto int_fields(smd.int_fields());
    auto str_fields(smd.str_fields());

    Shard shard(dir, int_fields, str_fields);

    for (auto field: int_fields) {
        dump_field<IntTerm>(shard, field, docid);
    }

    for (auto field: str_fields) {
        dump_field<StringTerm>(shard, field, docid);
    }
    cout << endl;
}
