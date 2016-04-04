/*
[GroupMultiRemapRules
  {target: 1 gmrr:
     [GroupMultiRemapRule negative: 1 rules: {
                 [Rule positive: 2 condition: [RegroupCondition kind: StrEquality
                                                                 field: q term: ]]                                    [Rule positive: 3 condition: [RegroupCondition kind: StrEquality
                                                                 field: q term: part time]] }
                                                                 ]}]
 */
#include "regroup.hpp"
#include "shard_metadata.hpp"

#include <boost/program_options.hpp>

#include <iostream>
#include <sstream>
#include <vector>

#define restrict __restrict__
extern "C" {
#include "imhotep_native.h"
#include "table_sizes.h"
#include "varintdecode.h"
}
using namespace imhotep;
using namespace std;

struct TableMetadata {
    int32_t* sizes           = 0;
    int32_t* vec_nums        = 0;
    int32_t* offsets_in_vecs = 0;

    TableMetadata(int num_cols,
                  const int64_t * restrict mins,
                  const int64_t * restrict maxes)
        : sizes(get_sizes(num_cols, mins, maxes))
        , vec_nums(get_vec_nums(num_cols, mins, maxes, sizes))
        , offsets_in_vecs(get_offsets_in_vecs(num_cols, mins, maxes, sizes))
    { }

    TableMetadata(const TableMetadata& rhs) = delete;

    ~TableMetadata() {
        free(sizes);
        free(vec_nums);
        free(offsets_in_vecs);
    }
};


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

    const string dir(vm["shard"].as<string>());

    const vector<string> empty;
    Shard shard(dir, empty, empty);
    const ShardMetadata smd(shard);
    const vector<string> int_fields(smd.int_fields());
    const vector<string> str_fields(smd.str_fields());

    vector<int64_t> mins{0};
    vector<int64_t> maxes{1};

    const size_t num_cols(1);
    TableMetadata tmd(num_cols, mins.data(), maxes.data());

    Shard::packed_table_ptr table =
        packed_table_create(smd.num_docs(),
                            mins.data(), maxes.data(),
                            tmd.sizes, tmd.vec_nums, tmd.offsets_in_vecs,
                            vector<int8_t>(num_cols, 0).data(), num_cols, 0);

    shard = Shard(dir, int_fields, str_fields, table);

    GroupMultiRemapRule::Rules rules;
    rules.push_back(GroupMultiRemapRule::Rule(38, StrEquality("qwords", "estágio")));
    rules.push_back(GroupMultiRemapRule::Rule(39, StrEquality("qwords", "auxiliar")));
    rules.push_back(GroupMultiRemapRule::Rule(40, StrEquality("qwords", "sine")));
    rules.push_back(GroupMultiRemapRule::Rule(41, StrEquality("qwords", "administrativo")));
    rules.push_back(GroupMultiRemapRule::Rule(42, StrEquality("qwords", "recepcionista")));
    rules.push_back(GroupMultiRemapRule::Rule(43, StrEquality("qwords", "técnico")));
    rules.push_back(GroupMultiRemapRule::Rule(44, StrEquality("qwords", "assistente")));
    rules.push_back(GroupMultiRemapRule::Rule(45, StrEquality("qwords", "vagas")));
    rules.push_back(GroupMultiRemapRule::Rule(46, StrEquality("qwords", "produção")));
    rules.push_back(GroupMultiRemapRule::Rule(47, StrEquality("qwords", "operador")));
    rules.push_back(GroupMultiRemapRule::Rule(48, StrEquality("qwords", "segurança")));
    rules.push_back(GroupMultiRemapRule::Rule(49, StrEquality("qwords", "analista")));
    rules.push_back(GroupMultiRemapRule::Rule(50, StrEquality("qwords", "aprendiz")));
    rules.push_back(GroupMultiRemapRule::Rule(51, StrEquality("qwords", "motorista")));
    rules.push_back(GroupMultiRemapRule::Rule(52, StrEquality("qwords", "jovem")));

    GroupMultiRemapRules gmrrs;
    gmrrs.emplace_back(make_pair(4, GroupMultiRemapRule(37, rules)));
    DocToGroup dtgs(table);
    for (DocToGroup::doc_t it(dtgs.begin()); it != dtgs.end(); ++it) {
        dtgs.set(it, 4);
    }

    //    cout << "before regroup num_groups: " << dtgs.num_groups() << endl;
    for (int count(0); count < 100; ++count) {
        Regroup regroup(gmrrs, shard, dtgs);
        regroup();
    }
    //    cout << " after regroup num_groups: " << dtgs.num_groups() << endl;

        /*
          for (DocToGroup::doc_t it(dtgs.begin()); it != dtgs.end(); ++it) {
          const DocToGroup::group_t group(dtgs.get(it));
          if (group > 0) cout << "doc: " << it << " group: " << group << endl;
          }
        */

    packed_table_destroy(table);
}
