#include "docid_iterator.hpp"
#include "regroup_condition.hpp"
#include "shard.hpp"
#include "shard_metadata.hpp"

#include <boost/program_options.hpp>
#include <yaml-cpp/yaml.h>

#include <iostream>
#include <unordered_map>
#include <vector>

#define restrict __restrict__
extern "C" {
#include "varintdecode.h"
}

using namespace imhotep;
using namespace std;

struct Eval : public boost::static_visitor<bool> {

    Eval(docid_t doc) : _doc(doc) { }

    template <typename Cond>
    bool operator()(Cond& cond) const {
        return cond.matches(_doc);
    }

private:
    docid_t _doc;
};

template <typename term_t>
vector<docid_t> docids_for(Shard& shard, const string& field, const term_t& term)
{
    vector<docid_t> result;
    VarIntView docid_view(shard.docid_view<term_t>(field));
    DocIdIterator it(docid_view, term.doc_offset(), term.doc_freq());
    DocIdIterator end;
    while (it != end) {
        result.push_back(*it);
        ++it;
    }
    return result;
}

template <typename term_t, typename id_t>
void check_field(Shard& shard, const string& field)
{
    TermIterator<term_t> it(shard.term_view<term_t>(field));
    TermIterator<term_t> end;

    typedef unordered_map<id_t, RegroupCondition> TermCondMap;
    TermCondMap terms_to_conds;
    while (it != end) {
        const id_t              id(it->id());
        EqualityCondition<id_t> cond(field, id);
        terms_to_conds[id] = cond;
        ++it;
    }

    const Reset reset(shard);
    for (typename TermCondMap::iterator it(terms_to_conds.begin());
         it != terms_to_conds.end(); ++it) {
        boost::apply_visitor(reset, it->second);
    }

    VarIntView docid_view(shard.docid_view<term_t>(field));

    it = shard.term_view<term_t>(field);
    while (it != end) {
        const term_t&         term(*it);
        const id_t            id(term.id());
        const vector<docid_t> docids(docids_for(shard, field, term));
        RegroupCondition&     cond(terms_to_conds[id]);

        for (auto docid: docids) {
            const Eval eval(docid);
            const bool result(boost::apply_visitor(eval, cond));
            if (!result) {
                cerr << "eval failed" << " docid: " << docid
                     << " cond: "  << cond << endl;
            }
        }
        ++it;
    }
}

int main(int argc, char* argv[]) {

    simdvbyteinit();

    namespace po = boost::program_options;

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

    const string         dir(vm["shard"].as<string>());
    Shard                shard(dir);
    const ShardMetadata  smd(shard);

    auto int_fields(smd.int_fields());
    auto str_fields(smd.str_fields());

    cerr << "int fields:" << endl;
    for (auto field: int_fields) {
        cerr << " " << field;
        check_field<IntTerm, int64_t>(shard, field);
    }

    cerr << endl << endl;

    cerr << "str fields:" << endl;
    for (auto field: str_fields) {
        cerr << " " << field;
        check_field<StringTerm, string>(shard, field);
    }
    cerr << endl;
}
