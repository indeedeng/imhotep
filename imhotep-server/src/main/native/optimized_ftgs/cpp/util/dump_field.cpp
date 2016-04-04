#include "docid_iterator.hpp"
#include "shard.hpp"
#include "shard_metadata.hpp"

#include <boost/program_options.hpp>

#include <algorithm>
#include <iostream>
#include <vector>

#define restrict __restrict__
extern "C" {
#include "varintdecode.h"
}

using namespace imhotep;
using namespace std;

template <typename term_t>
void dump_docs(VarIntView docid_view, TermIterator<term_t> it)
{
    DocIdIterator docid_it(docid_view, it->doc_offset(), it->doc_freq());
    DocIdIterator docid_end;
    while (docid_it != docid_end) {
        cout << " " << *docid_it;
        ++docid_it;
    }
}

template <typename term_t>
void dump_field(Shard& shard, const string& field)
{
    VarIntView           docid_view(shard.docid_view<term_t>(field));
    TermIterator<term_t> it(shard.term_view<term_t>(field));
    TermIterator<term_t> end;

    vector<id_t> terms;
    while (it != end) {
        cout << (*it).id() << ":";
        dump_docs(docid_view, it);
        cout << endl;
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
        ("field", po::value<string>(), "field to dump")
        ("shard", po::value<string>(), "shard directory")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help") || !vm.count("shard")) {
        cerr << desc << endl;
        return 0;
    }

    const string        dir(vm["shard"].as<string>());
    const string        field(vm["field"].as<string>());
    const Shard         proto_shard(dir, empty, empty);
    const ShardMetadata smd(proto_shard);

    auto int_fields(smd.int_fields());
    auto str_fields(smd.str_fields());

    Shard shard(dir, int_fields, str_fields);

    for (auto int_field: int_fields) {
        if (field == int_field) {
            dump_field<IntTerm>(shard, field);
        }
    }

    for (auto str_field: str_fields) {
        if (field == str_field) {
            dump_field<StringTerm>(shard, field);
        }
    }
}
