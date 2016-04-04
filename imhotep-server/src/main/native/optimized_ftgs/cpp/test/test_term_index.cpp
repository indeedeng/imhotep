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

template <typename term_t, typename id_t>
void check_each_term(Shard&              shard,
                     const string&       field,
                     const vector<id_t>& terms)
{ }

template <>
void check_each_term<IntTerm, IntTerm::id_t>(Shard&                       shard,
                                             const string&                field,
                                             const vector<IntTerm::id_t>& terms)
{
    const IntTermIndex term_idx(shard.int_term_index(field));
    for (const IntTerm::id_t& term: terms) {
        TermIterator<IntTerm> it(term_idx.find_it(term));
        if (it->id() != term) {
            cerr << "error: term mismatch looking for: " << term
                 << " found: " << it->id() << endl;
        }
    }
}

template <>
void check_each_term<StringTerm, string>(Shard&                shard,
                                         const string&         field,
                                         const vector<string>& terms)
{
    const StringTermIndex term_idx(shard.str_term_index(field));
    for (const string& term: terms) {
        TermIterator<StringTerm> it(term_idx.find_it(term));
        const string term_found(static_cast<string>(it->id()));
        if (term_found != term) {
            cerr << "error: term mismatch looking for: " << term
                 << " found: " << term_found << endl;
        }
    }
}


template <typename term_t, typename id_t>
void check_field(Shard& shard, const string& field)
{
    VarIntView           docid_view(shard.docid_view<term_t>(field));
    TermIterator<term_t> it(shard.term_view<term_t>(field));
    TermIterator<term_t> end;

    vector<id_t> terms;
    while (it != end) {
        terms.push_back(static_cast<id_t>(it->id()));
         ++it;
    }
    random_shuffle(terms.begin(), terms.end());
    check_each_term<term_t>(shard, field, terms);
}

void dump_docs(VarIntView docid_view, TermIterator<IntTerm> it)
{
    DocIdIterator docid_it(docid_view, it->doc_offset(), it->doc_freq());
    DocIdIterator docid_end;
    while (docid_it != docid_end) {
        cout << "   " << *docid_it << endl;
        ++docid_it;
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
        ("shard",  po::value<string>(), "shard directory")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help") || !vm.count("shard")) {
        cerr << desc << endl;
        return 0;
    }

    const string        dir(vm["shard"].as<string>());
    const Shard         proto_shard(dir, empty, empty);
    const ShardMetadata smd(proto_shard);

    auto int_fields(smd.int_fields());
    auto str_fields(smd.str_fields());

    Shard shard(dir, int_fields, str_fields);

    cerr << "int fields:" << endl;
    for (auto field: int_fields) {
        cerr << " " << field;
        check_field<IntTerm, IntTerm::id_t>(shard, field);
    }

    cerr << endl << endl;

    cerr << "str fields:" << endl;
    for (auto field: str_fields) {
        cerr << " " << field;
        check_field<StringTerm, string>(shard, field);
    }
    cerr << endl;
}
