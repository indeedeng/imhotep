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

GroupMultiRemapRules canned_rules();

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

    Shard                proto_shard(dir);
    const ShardMetadata  smd(proto_shard);
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

    Shard shard(dir, table);

    GroupMultiRemapRules gmrrs(canned_rules());
    DocToGroup dtgs(table);
    cerr << "gmrrs.size(): " << gmrrs.size() << endl;
    for (DocToGroup::doc_t it(dtgs.begin()); it != dtgs.end(); ++it) {
        dtgs.set(it, it % gmrrs.size());
    }

    //    cout << "before regroup num_groups: " << dtgs.num_groups() << endl;

    // for (int count(0); count < 100; ++count) {
    Regroup regroup(gmrrs, shard, dtgs);
    regroup();
    // }

    //    cout << " after regroup num_groups: " << dtgs.num_groups() << endl;

    /*
      for (DocToGroup::doc_t it(dtgs.begin()); it != dtgs.end(); ++it) {
      const DocToGroup::group_t group(dtgs.get(it));
      if (group > 0) cout << "doc: " << it << " group: " << group << endl;
      }
    */

    packed_table_destroy(table);
}

/** !@# Something of a crazy test hack here. These are canned rules
    extracted from a test ctrmodel build with a makeshift code
    generator.
*/
GroupMultiRemapRules canned_rules() {
    GroupMultiRemapRules gmrrs;
    GroupMultiRemapRule::Rules rules;
    rules.push_back(GroupMultiRemapRule::Rule(2, StrEquality("country","au")));
    rules.push_back(GroupMultiRemapRule::Rule(3, StrEquality("country","nl")));
    rules.push_back(GroupMultiRemapRule::Rule(4, StrEquality("country","co")));
    rules.push_back(GroupMultiRemapRule::Rule(5, StrEquality("country","mx")));
    rules.push_back(GroupMultiRemapRule::Rule(6, StrEquality("country","pe")));
    rules.push_back(GroupMultiRemapRule::Rule(7, StrEquality("country","cl")));
    rules.push_back(GroupMultiRemapRule::Rule(8, StrEquality("country","tr")));
    rules.push_back(GroupMultiRemapRule::Rule(9, StrEquality("country","ch")));
    rules.push_back(GroupMultiRemapRule::Rule(10, StrEquality("country","ae")));
    rules.push_back(GroupMultiRemapRule::Rule(11, StrEquality("country","kr")));
    rules.push_back(GroupMultiRemapRule::Rule(12, StrEquality("country","be")));
    rules.push_back(GroupMultiRemapRule::Rule(13, StrEquality("country","ie")));
    rules.push_back(GroupMultiRemapRule::Rule(14, StrEquality("country","at")));
    rules.push_back(GroupMultiRemapRule::Rule(15, StrEquality("country","pl")));
    rules.push_back(GroupMultiRemapRule::Rule(16, StrEquality("country","ph")));
    rules.push_back(GroupMultiRemapRule::Rule(17, StrEquality("country","id")));
    gmrrs.emplace_back(make_pair(1, GroupMultiRemapRule(1, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(19, IntEquality("tlonet",43)));
    rules.push_back(GroupMultiRemapRule::Rule(20, IntEquality("tlonet",41)));
    rules.push_back(GroupMultiRemapRule::Rule(21, IntEquality("tlonet",11)));
    rules.push_back(GroupMultiRemapRule::Rule(22, IntEquality("tlonet",21)));
    rules.push_back(GroupMultiRemapRule::Rule(23, IntEquality("tlonet",19)));
    rules.push_back(GroupMultiRemapRule::Rule(24, IntEquality("tlonet",35)));
    rules.push_back(GroupMultiRemapRule::Rule(25, IntEquality("tlonet",27)));
    rules.push_back(GroupMultiRemapRule::Rule(26, IntEquality("tlonet",49)));
    rules.push_back(GroupMultiRemapRule::Rule(27, IntEquality("tlonet",29)));
    rules.push_back(GroupMultiRemapRule::Rule(28, IntEquality("tlonet",33)));
    rules.push_back(GroupMultiRemapRule::Rule(29, IntEquality("tlonet",25)));
    rules.push_back(GroupMultiRemapRule::Rule(30, IntEquality("tlonet",39)));
    rules.push_back(GroupMultiRemapRule::Rule(31, IntEquality("tlonet",53)));
    rules.push_back(GroupMultiRemapRule::Rule(32, IntEquality("tlonet",17)));
    rules.push_back(GroupMultiRemapRule::Rule(33, IntEquality("tlonet",31)));
    rules.push_back(GroupMultiRemapRule::Rule(34, IntEquality("tlonet",13)));
    gmrrs.emplace_back(make_pair(2, GroupMultiRemapRule(18, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(36, StrEquality("qwords","engineer")));
    gmrrs.emplace_back(make_pair(3, GroupMultiRemapRule(35, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(38, StrEquality("qwords","estágio")));
    rules.push_back(GroupMultiRemapRule::Rule(39, StrEquality("qwords","auxiliar")));
    rules.push_back(GroupMultiRemapRule::Rule(40, StrEquality("qwords","sine")));
    rules.push_back(GroupMultiRemapRule::Rule(41, StrEquality("qwords","administrativo")));
    rules.push_back(GroupMultiRemapRule::Rule(42, StrEquality("qwords","recepcionista")));
    rules.push_back(GroupMultiRemapRule::Rule(43, StrEquality("qwords","técnico")));
    rules.push_back(GroupMultiRemapRule::Rule(44, StrEquality("qwords","assistente")));
    rules.push_back(GroupMultiRemapRule::Rule(45, StrEquality("qwords","vagas")));
    rules.push_back(GroupMultiRemapRule::Rule(46, StrEquality("qwords","produção")));
    rules.push_back(GroupMultiRemapRule::Rule(47, StrEquality("qwords","operador")));
    rules.push_back(GroupMultiRemapRule::Rule(48, StrEquality("qwords","segurança")));
    rules.push_back(GroupMultiRemapRule::Rule(49, StrEquality("qwords","analista")));
    rules.push_back(GroupMultiRemapRule::Rule(50, StrEquality("qwords","aprendiz")));
    rules.push_back(GroupMultiRemapRule::Rule(51, StrEquality("qwords","motorista")));
    rules.push_back(GroupMultiRemapRule::Rule(52, StrEquality("qwords","jovem")));
    gmrrs.emplace_back(make_pair(4, GroupMultiRemapRule(37, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(54, IntEquality("sourceid",40096)));
    rules.push_back(GroupMultiRemapRule::Rule(55, IntEquality("sourceid",94930)));
    rules.push_back(GroupMultiRemapRule::Rule(56, IntEquality("sourceid",51461)));
    rules.push_back(GroupMultiRemapRule::Rule(57, IntEquality("sourceid",77621)));
    rules.push_back(GroupMultiRemapRule::Rule(58, IntEquality("sourceid",40147)));
    rules.push_back(GroupMultiRemapRule::Rule(59, IntEquality("sourceid",63770)));
    gmrrs.emplace_back(make_pair(5, GroupMultiRemapRule(53, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(61, IntEquality("tlonet",11)));
    rules.push_back(GroupMultiRemapRule::Rule(62, IntEquality("tlonet",43)));
    rules.push_back(GroupMultiRemapRule::Rule(63, IntEquality("tlonet",41)));
    rules.push_back(GroupMultiRemapRule::Rule(64, IntEquality("tlonet",47)));
    rules.push_back(GroupMultiRemapRule::Rule(65, IntEquality("tlonet",21)));
    rules.push_back(GroupMultiRemapRule::Rule(66, IntEquality("tlonet",19)));
    rules.push_back(GroupMultiRemapRule::Rule(67, IntEquality("tlonet",49)));
    rules.push_back(GroupMultiRemapRule::Rule(68, IntEquality("tlonet",35)));
    rules.push_back(GroupMultiRemapRule::Rule(69, IntEquality("tlonet",53)));
    rules.push_back(GroupMultiRemapRule::Rule(70, IntEquality("tlonet",31)));
    rules.push_back(GroupMultiRemapRule::Rule(71, IntEquality("tlonet",33)));
    rules.push_back(GroupMultiRemapRule::Rule(72, IntEquality("tlonet",15)));
    gmrrs.emplace_back(make_pair(6, GroupMultiRemapRule(60, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(74, StrEquality("qwords","all")));
    rules.push_back(GroupMultiRemapRule::Rule(75, StrEquality("qwords","jobs")));
    rules.push_back(GroupMultiRemapRule::Rule(76, StrEquality("qwords","warehouse")));
    rules.push_back(GroupMultiRemapRule::Rule(77, StrEquality("qwords","retail")));
    rules.push_back(GroupMultiRemapRule::Rule(78, StrEquality("qwords","engineer")));
    rules.push_back(GroupMultiRemapRule::Rule(79, StrEquality("qwords","full")));
    rules.push_back(GroupMultiRemapRule::Rule(80, StrEquality("qwords","time")));
    rules.push_back(GroupMultiRemapRule::Rule(81, StrEquality("qwords","driver")));
    rules.push_back(GroupMultiRemapRule::Rule(82, StrEquality("qwords","receptionist")));
    rules.push_back(GroupMultiRemapRule::Rule(83, StrEquality("qwords","graduate")));
    rules.push_back(GroupMultiRemapRule::Rule(84, StrEquality("qwords","£20,000")));
    rules.push_back(GroupMultiRemapRule::Rule(85, StrEquality("qwords","cleaner")));
    rules.push_back(GroupMultiRemapRule::Rule(86, StrEquality("qwords","worker")));
    rules.push_back(GroupMultiRemapRule::Rule(87, StrEquality("qwords","support")));
    rules.push_back(GroupMultiRemapRule::Rule(88, StrEquality("qwords","care")));
    rules.push_back(GroupMultiRemapRule::Rule(89, StrEquality("qwords","security")));
    rules.push_back(GroupMultiRemapRule::Rule(90, StrEquality("qwords","sales")));
    rules.push_back(GroupMultiRemapRule::Rule(91, StrEquality("qwords","assistant")));
    rules.push_back(GroupMultiRemapRule::Rule(92, StrEquality("qwords","trainee")));
    rules.push_back(GroupMultiRemapRule::Rule(93, StrEquality("qwords","part")));
    gmrrs.emplace_back(make_pair(7, GroupMultiRemapRule(73, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(95, StrEquality("qwords","stage")));
    rules.push_back(GroupMultiRemapRule::Rule(96, StrEquality("qwords","comptable")));
    rules.push_back(GroupMultiRemapRule::Rule(97, StrEquality("qwords","assistant")));
    rules.push_back(GroupMultiRemapRule::Rule(98, StrEquality("qwords","social")));
    rules.push_back(GroupMultiRemapRule::Rule(99, StrEquality("qwords","informatique")));
    rules.push_back(GroupMultiRemapRule::Rule(100, StrEquality("qwords","secrétaire")));
    rules.push_back(GroupMultiRemapRule::Rule(101, StrEquality("qwords","chauffeur")));
    rules.push_back(GroupMultiRemapRule::Rule(102, StrEquality("qwords","humaines")));
    rules.push_back(GroupMultiRemapRule::Rule(103, StrEquality("qwords","assistante")));
    rules.push_back(GroupMultiRemapRule::Rule(104, StrEquality("qwords","technicien")));
    rules.push_back(GroupMultiRemapRule::Rule(105, StrEquality("qwords","ressources")));
    rules.push_back(GroupMultiRemapRule::Rule(106, StrEquality("qwords","gestion")));
    rules.push_back(GroupMultiRemapRule::Rule(107, StrEquality("qwords","commercial")));
    rules.push_back(GroupMultiRemapRule::Rule(108, StrEquality("qwords","infirmier")));
    rules.push_back(GroupMultiRemapRule::Rule(109, StrEquality("qwords","marketing")));
    gmrrs.emplace_back(make_pair(8, GroupMultiRemapRule(94, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(111, IntEquality("sourceid",28026)));
    rules.push_back(GroupMultiRemapRule::Rule(112, IntEquality("sourceid",96882)));
    gmrrs.emplace_back(make_pair(9, GroupMultiRemapRule(110, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(114, IntEquality("companyid",377805)));
    gmrrs.emplace_back(make_pair(10, GroupMultiRemapRule(113, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(116, StrEquality("country","it")));
    rules.push_back(GroupMultiRemapRule::Rule(117, StrEquality("country","us")));
    rules.push_back(GroupMultiRemapRule::Rule(118, StrEquality("country","hu")));
    rules.push_back(GroupMultiRemapRule::Rule(119, StrEquality("country","gb")));
    rules.push_back(GroupMultiRemapRule::Rule(120, StrEquality("country","pl")));
    gmrrs.emplace_back(make_pair(11, GroupMultiRemapRule(115, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(122, IntEquality("onetid",478)));
    rules.push_back(GroupMultiRemapRule::Rule(123, IntEquality("onetid",488)));
    rules.push_back(GroupMultiRemapRule::Rule(124, IntEquality("onetid",513)));
    rules.push_back(GroupMultiRemapRule::Rule(125, IntEquality("onetid",508)));
    rules.push_back(GroupMultiRemapRule::Rule(126, IntEquality("onetid",499)));
    rules.push_back(GroupMultiRemapRule::Rule(127, IntEquality("onetid",497)));
    rules.push_back(GroupMultiRemapRule::Rule(128, IntEquality("onetid",473)));
    rules.push_back(GroupMultiRemapRule::Rule(129, IntEquality("onetid",503)));
    rules.push_back(GroupMultiRemapRule::Rule(130, IntEquality("onetid",463)));
    rules.push_back(GroupMultiRemapRule::Rule(131, IntEquality("onetid",467)));
    rules.push_back(GroupMultiRemapRule::Rule(132, IntEquality("onetid",487)));
    rules.push_back(GroupMultiRemapRule::Rule(133, IntEquality("onetid",471)));
    gmrrs.emplace_back(make_pair(12, GroupMultiRemapRule(121, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(135, StrEquality("country","us")));
    rules.push_back(GroupMultiRemapRule::Rule(136, StrEquality("country","gb")));
    rules.push_back(GroupMultiRemapRule::Rule(137, StrEquality("country","au")));
    rules.push_back(GroupMultiRemapRule::Rule(138, StrEquality("country","it")));
    gmrrs.emplace_back(make_pair(13, GroupMultiRemapRule(134, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(140, StrEquality("country","us")));
    rules.push_back(GroupMultiRemapRule::Rule(141, StrEquality("country","gb")));
    rules.push_back(GroupMultiRemapRule::Rule(142, StrEquality("country","fr")));
    rules.push_back(GroupMultiRemapRule::Rule(143, StrEquality("country","au")));
    rules.push_back(GroupMultiRemapRule::Rule(144, StrEquality("country","br")));
    rules.push_back(GroupMultiRemapRule::Rule(145, StrEquality("country","it")));
    rules.push_back(GroupMultiRemapRule::Rule(146, StrEquality("country","ca")));
    rules.push_back(GroupMultiRemapRule::Rule(147, StrEquality("country","pl")));
    gmrrs.emplace_back(make_pair(14, GroupMultiRemapRule(139, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(149, StrEquality("country","us")));
    rules.push_back(GroupMultiRemapRule::Rule(150, StrEquality("country","jp")));
    rules.push_back(GroupMultiRemapRule::Rule(151, StrEquality("country","de")));
    rules.push_back(GroupMultiRemapRule::Rule(152, StrEquality("country","it")));
    rules.push_back(GroupMultiRemapRule::Rule(153, StrEquality("country","br")));
    rules.push_back(GroupMultiRemapRule::Rule(154, StrEquality("country","pl")));
    rules.push_back(GroupMultiRemapRule::Rule(155, StrEquality("country","nl")));
    rules.push_back(GroupMultiRemapRule::Rule(156, StrEquality("country","gb")));
    gmrrs.emplace_back(make_pair(15, GroupMultiRemapRule(148, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(158, IntEquality("onetid",375)));
    rules.push_back(GroupMultiRemapRule::Rule(159, IntEquality("onetid",370)));
    rules.push_back(GroupMultiRemapRule::Rule(160, IntEquality("onetid",373)));
    rules.push_back(GroupMultiRemapRule::Rule(161, IntEquality("onetid",366)));
    rules.push_back(GroupMultiRemapRule::Rule(162, IntEquality("onetid",372)));
    rules.push_back(GroupMultiRemapRule::Rule(163, IntEquality("onetid",367)));
    rules.push_back(GroupMultiRemapRule::Rule(164, IntEquality("onetid",377)));
    rules.push_back(GroupMultiRemapRule::Rule(165, IntEquality("onetid",378)));
    rules.push_back(GroupMultiRemapRule::Rule(166, IntEquality("onetid",369)));
    gmrrs.emplace_back(make_pair(16, GroupMultiRemapRule(157, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(168, IntEquality("onetid",70)));
    rules.push_back(GroupMultiRemapRule::Rule(169, IntEquality("onetid",71)));
    rules.push_back(GroupMultiRemapRule::Rule(170, IntEquality("onetid",78)));
    rules.push_back(GroupMultiRemapRule::Rule(171, IntEquality("onetid",69)));
    rules.push_back(GroupMultiRemapRule::Rule(172, IntEquality("onetid",68)));
    rules.push_back(GroupMultiRemapRule::Rule(173, IntEquality("onetid",75)));
    rules.push_back(GroupMultiRemapRule::Rule(174, IntEquality("onetid",76)));
    rules.push_back(GroupMultiRemapRule::Rule(175, IntEquality("onetid",67)));
    gmrrs.emplace_back(make_pair(17, GroupMultiRemapRule(167, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(177, StrEquality("country","us")));
    rules.push_back(GroupMultiRemapRule::Rule(178, StrEquality("country","vn")));
    rules.push_back(GroupMultiRemapRule::Rule(179, StrEquality("country","nl")));
    rules.push_back(GroupMultiRemapRule::Rule(180, StrEquality("country","pl")));
    rules.push_back(GroupMultiRemapRule::Rule(181, StrEquality("country","au")));
    rules.push_back(GroupMultiRemapRule::Rule(182, StrEquality("country","gb")));
    gmrrs.emplace_back(make_pair(18, GroupMultiRemapRule(176, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(184, StrEquality("country","gb")));
    rules.push_back(GroupMultiRemapRule::Rule(185, StrEquality("country","mx")));
    rules.push_back(GroupMultiRemapRule::Rule(186, StrEquality("country","us")));
    rules.push_back(GroupMultiRemapRule::Rule(187, StrEquality("country","br")));
    rules.push_back(GroupMultiRemapRule::Rule(188, StrEquality("country","fr")));
    rules.push_back(GroupMultiRemapRule::Rule(189, StrEquality("country","pl")));
    rules.push_back(GroupMultiRemapRule::Rule(190, StrEquality("country","de")));
    gmrrs.emplace_back(make_pair(19, GroupMultiRemapRule(183, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(192, IntEquality("onetid",435)));
    rules.push_back(GroupMultiRemapRule::Rule(193, IntEquality("onetid",417)));
    rules.push_back(GroupMultiRemapRule::Rule(194, IntEquality("onetid",411)));
    rules.push_back(GroupMultiRemapRule::Rule(195, IntEquality("onetid",424)));
    rules.push_back(GroupMultiRemapRule::Rule(196, IntEquality("onetid",407)));
    rules.push_back(GroupMultiRemapRule::Rule(197, IntEquality("onetid",430)));
    rules.push_back(GroupMultiRemapRule::Rule(198, IntEquality("onetid",422)));
    rules.push_back(GroupMultiRemapRule::Rule(199, IntEquality("onetid",434)));
    rules.push_back(GroupMultiRemapRule::Rule(200, IntEquality("onetid",438)));
    rules.push_back(GroupMultiRemapRule::Rule(201, IntEquality("onetid",436)));
    rules.push_back(GroupMultiRemapRule::Rule(202, IntEquality("onetid",437)));
    rules.push_back(GroupMultiRemapRule::Rule(203, IntEquality("onetid",416)));
    rules.push_back(GroupMultiRemapRule::Rule(204, IntEquality("onetid",419)));
    rules.push_back(GroupMultiRemapRule::Rule(205, IntEquality("onetid",428)));
    gmrrs.emplace_back(make_pair(20, GroupMultiRemapRule(191, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(207, IntEquality("onetid",765)));
    rules.push_back(GroupMultiRemapRule::Rule(208, IntEquality("onetid",764)));
    rules.push_back(GroupMultiRemapRule::Rule(209, IntEquality("onetid",796)));
    rules.push_back(GroupMultiRemapRule::Rule(210, IntEquality("onetid",794)));
    rules.push_back(GroupMultiRemapRule::Rule(211, IntEquality("onetid",782)));
    rules.push_back(GroupMultiRemapRule::Rule(212, IntEquality("onetid",758)));
    rules.push_back(GroupMultiRemapRule::Rule(213, IntEquality("onetid",784)));
    rules.push_back(GroupMultiRemapRule::Rule(214, IntEquality("onetid",767)));
    rules.push_back(GroupMultiRemapRule::Rule(215, IntEquality("onetid",759)));
    rules.push_back(GroupMultiRemapRule::Rule(216, IntEquality("onetid",795)));
    rules.push_back(GroupMultiRemapRule::Rule(217, IntEquality("onetid",763)));
    gmrrs.emplace_back(make_pair(21, GroupMultiRemapRule(206, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(219, StrEquality("country","us")));
    rules.push_back(GroupMultiRemapRule::Rule(220, StrEquality("country","ca")));
    rules.push_back(GroupMultiRemapRule::Rule(221, StrEquality("country","gb")));
    rules.push_back(GroupMultiRemapRule::Rule(222, StrEquality("country","fr")));
    gmrrs.emplace_back(make_pair(22, GroupMultiRemapRule(218, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(224, StrEquality("country","us")));
    rules.push_back(GroupMultiRemapRule::Rule(225, StrEquality("country","gb")));
    rules.push_back(GroupMultiRemapRule::Rule(226, StrEquality("country","be")));
    rules.push_back(GroupMultiRemapRule::Rule(227, StrEquality("country","mx")));
    rules.push_back(GroupMultiRemapRule::Rule(228, StrEquality("country","au")));
    rules.push_back(GroupMultiRemapRule::Rule(229, StrEquality("country","jp")));
    rules.push_back(GroupMultiRemapRule::Rule(230, StrEquality("country","it")));
    rules.push_back(GroupMultiRemapRule::Rule(231, StrEquality("country","ch")));
    rules.push_back(GroupMultiRemapRule::Rule(232, StrEquality("country","br")));
    gmrrs.emplace_back(make_pair(23, GroupMultiRemapRule(223, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(234, StrEquality("country","us")));
    rules.push_back(GroupMultiRemapRule::Rule(235, StrEquality("country","pl")));
    rules.push_back(GroupMultiRemapRule::Rule(236, StrEquality("country","it")));
    rules.push_back(GroupMultiRemapRule::Rule(237, StrEquality("country","fr")));
    gmrrs.emplace_back(make_pair(24, GroupMultiRemapRule(233, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(239, IntEquality("onetid",125)));
    rules.push_back(GroupMultiRemapRule::Rule(240, IntEquality("onetid",142)));
    rules.push_back(GroupMultiRemapRule::Rule(241, IntEquality("onetid",160)));
    rules.push_back(GroupMultiRemapRule::Rule(242, IntEquality("onetid",157)));
    rules.push_back(GroupMultiRemapRule::Rule(243, IntEquality("onetid",123)));
    gmrrs.emplace_back(make_pair(25, GroupMultiRemapRule(238, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(245, StrEquality("country","us")));
    rules.push_back(GroupMultiRemapRule::Rule(246, StrEquality("country","ru")));
    rules.push_back(GroupMultiRemapRule::Rule(247, StrEquality("country","jp")));
    rules.push_back(GroupMultiRemapRule::Rule(248, StrEquality("country","br")));
    rules.push_back(GroupMultiRemapRule::Rule(249, StrEquality("country","in")));
    rules.push_back(GroupMultiRemapRule::Rule(250, StrEquality("country","pl")));
    rules.push_back(GroupMultiRemapRule::Rule(251, StrEquality("country","au")));
    rules.push_back(GroupMultiRemapRule::Rule(252, StrEquality("country","gb")));
    rules.push_back(GroupMultiRemapRule::Rule(253, StrEquality("country","fr")));
    gmrrs.emplace_back(make_pair(26, GroupMultiRemapRule(244, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(255, StrEquality("country","us")));
    rules.push_back(GroupMultiRemapRule::Rule(256, StrEquality("country","ca")));
    rules.push_back(GroupMultiRemapRule::Rule(257, StrEquality("country","au")));
    gmrrs.emplace_back(make_pair(27, GroupMultiRemapRule(254, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(259, IntEquality("companyid",44809190)));
    gmrrs.emplace_back(make_pair(28, GroupMultiRemapRule(258, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(261, StrEquality("country","us")));
    rules.push_back(GroupMultiRemapRule::Rule(262, StrEquality("country","it")));
    rules.push_back(GroupMultiRemapRule::Rule(263, StrEquality("country","de")));
    rules.push_back(GroupMultiRemapRule::Rule(264, StrEquality("country","gb")));
    rules.push_back(GroupMultiRemapRule::Rule(265, StrEquality("country","be")));
    rules.push_back(GroupMultiRemapRule::Rule(266, StrEquality("country","au")));
    rules.push_back(GroupMultiRemapRule::Rule(267, StrEquality("country","nl")));
    rules.push_back(GroupMultiRemapRule::Rule(268, StrEquality("country","ch")));
    gmrrs.emplace_back(make_pair(29, GroupMultiRemapRule(260, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(270, IntEquality("companyid",4419)));
    gmrrs.emplace_back(make_pair(30, GroupMultiRemapRule(269, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(272, IntEquality("titleid",33428355)));
    gmrrs.emplace_back(make_pair(31, GroupMultiRemapRule(271, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(274, IntEquality("onetid",335)));
    rules.push_back(GroupMultiRemapRule::Rule(275, IntEquality("onetid",309)));
    rules.push_back(GroupMultiRemapRule::Rule(276, IntEquality("onetid",310)));
    rules.push_back(GroupMultiRemapRule::Rule(277, IntEquality("onetid",307)));
    gmrrs.emplace_back(make_pair(32, GroupMultiRemapRule(273, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(279, StrEquality("country","cl")));
    rules.push_back(GroupMultiRemapRule::Rule(280, StrEquality("country","in")));
    rules.push_back(GroupMultiRemapRule::Rule(281, StrEquality("country","gb")));
    rules.push_back(GroupMultiRemapRule::Rule(282, StrEquality("country","us")));
    rules.push_back(GroupMultiRemapRule::Rule(283, StrEquality("country","it")));
    gmrrs.emplace_back(make_pair(33, GroupMultiRemapRule(278, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(285, IntEquality("onetid",169)));
    rules.push_back(GroupMultiRemapRule::Rule(286, IntEquality("onetid",174)));
    gmrrs.emplace_back(make_pair(34, GroupMultiRemapRule(284, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(288, StrEquality("country","gb")));
    rules.push_back(GroupMultiRemapRule::Rule(289, StrEquality("country","us")));
    gmrrs.emplace_back(make_pair(35, GroupMultiRemapRule(287, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(291, IntEquality("onetid",765)));
    rules.push_back(GroupMultiRemapRule::Rule(292, IntEquality("onetid",794)));
    rules.push_back(GroupMultiRemapRule::Rule(293, IntEquality("onetid",796)));
    gmrrs.emplace_back(make_pair(36, GroupMultiRemapRule(290, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(295, IntEquality("companyid",377805)));
    gmrrs.emplace_back(make_pair(38, GroupMultiRemapRule(294, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(297, IntEquality("companyid",130402)));
    gmrrs.emplace_back(make_pair(39, GroupMultiRemapRule(296, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(299, StrEquality("country","us")));
    rules.push_back(GroupMultiRemapRule::Rule(300, StrEquality("country","ca")));
    rules.push_back(GroupMultiRemapRule::Rule(301, StrEquality("country","gb")));
    rules.push_back(GroupMultiRemapRule::Rule(302, StrEquality("country","au")));
    gmrrs.emplace_back(make_pair(40, GroupMultiRemapRule(298, rules)));
    rules.clear();
    rules.push_back(GroupMultiRemapRule::Rule(304, IntEquality("companyid",377805)));
    gmrrs.emplace_back(make_pair(41, GroupMultiRemapRule(303, rules)));
    return gmrrs;
}
