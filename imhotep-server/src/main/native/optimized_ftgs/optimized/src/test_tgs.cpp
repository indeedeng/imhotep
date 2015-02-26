#define restrict __restrict__
extern "C" {
#include "imhotep_native.h"
}

#include <algorithm>
#include <array>
#include <functional>
#include <set>
#include <vector>

using namespace std;

typedef function<int64_t(size_t doc_id)> GroupFunc;
typedef function<int64_t(int64_t min, int64_t max)> MetricFunc;

typedef vector<int>  DocIds;
typedef set<int64_t> GroupIds;

template <size_t n_metrics>
struct Shard {
  const int      _n_docs;
  packed_shard_t _shard;
  GroupIds       _gids;

  typedef array<int64_t, n_metrics> Limits;

  Shard(int        n_docs,
        Limits&    mins,
        Limits&    maxes,
        GroupFunc  group_func,
        MetricFunc metric_func)
    : _n_docs(n_docs) {

    packed_shard_init(&_shard, n_docs, mins.data(), maxes.data(), n_metrics);

    DocIds          doc_ids;
    vector<int64_t> gids_vector;
    for (int doc_id(0); doc_id < n_docs; ++doc_id) {
      doc_ids.push_back(doc_id);
      const int64_t gid(group_func(doc_id));
      _gids.insert(gid);
      gids_vector.push_back(gid);
    }

    packed_shard_update_groups(&_shard, doc_ids.data(), doc_ids.size(), gids_vector.data());

    vector<int64_t> metrics(n_metrics, 0);
    for (int metric_index(0); metric_index < n_metrics; ++metric_index) {
      vector<int64_t> metrics(doc_ids.size(), metric_func(mins[metric_index], maxes[metric_index]));
      packed_shard_update_metric(&_shard,
                                 doc_ids.data(), doc_ids.size(),
                                 metrics.data(), metric_index);
    }
  }

  ~Shard() { packed_shard_destroy(&_shard); }

  packed_shard_t* operator()() { return &_shard; };
};

int main(int argc, char* argv[])
{
  typedef Shard<42> TestShard;

  TestShard::Limits mins, maxes;
  fill(mins.begin(), mins.end(), 0);
  for (size_t i(0); i < maxes.size(); ++i) {
    maxes[i] = 1 << (i % 63);
  }

  constexpr size_t num_stats = 42;
  Shard<num_stats> shard(100, mins, maxes,
                         [](size_t doc_id) { return doc_id % 10; },
                         [](int64_t min, int64_t max) { return (min + max) / 2; });

  struct worker_desc worker;
  char *begin(reinterpret_cast<char *>(&worker)), *end(begin + sizeof(worker));
  fill(begin, end, 0);

  array<struct index_slice_info, 1> slice_infos({
      { { shard._n_docs, nullptr, shard() } }
    });
  struct tgs_desc tgs_desc;
  tgs_desc.n_slices        = slice_infos.size();
  tgs_desc.trm_slice_infos = slice_infos.data();

  struct session_desc session;
  session.num_groups       = shard._gids.size();
  session.num_stats        = num_stats;
  session.current_tgs_pass = &tgs_desc;

  tgs_execute_pass(&worker, &session, &tgs_desc);
}
