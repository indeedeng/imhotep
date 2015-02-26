#define restrict __restrict__
extern "C" {
#include "imhotep_native.h"
}

#include "test_utils.h"

#include <algorithm>
#include <array>
#include <functional>
#include <iostream>
#include <set>
#include <vector>

using namespace std;

typedef function<int64_t(size_t doc_id)> GroupFunc;
typedef function<int64_t(int64_t min, int64_t max)> MetricFunc;

typedef vector<int>  DocIds;
typedef set<int64_t> GroupIds;

template <size_t n_metrics>
struct Shard {
  packed_shard_t _shard;
  DocIds         _doc_ids;
  GroupIds       _gids;

  typedef array<int64_t, n_metrics> Limits;

  Shard(int        n_docs,
        Limits&    mins,
        Limits&    maxes,
        GroupFunc  group_func,
        MetricFunc metric_func) {

    packed_shard_init(&_shard, n_docs, mins.data(), maxes.data(), n_metrics);

    vector<int64_t> gids_vector;
    for (int doc_id(0); doc_id < n_docs; ++doc_id) {
      _doc_ids.push_back(doc_id);
      const int64_t gid(group_func(doc_id));
      _gids.insert(gid);
      gids_vector.push_back(gid);
    }

    packed_shard_update_groups(&_shard, _doc_ids.data(), _doc_ids.size(), gids_vector.data());

    vector<int64_t> metrics(n_metrics, 0);
    for (int metric_index(0); metric_index < n_metrics; ++metric_index) {
      vector<int64_t> metrics(_doc_ids.size(), metric_func(mins[metric_index], maxes[metric_index]));
      packed_shard_update_metric(&_shard,
                                 _doc_ids.data(), _doc_ids.size(),
                                 metrics.data(), metric_index);
    }
  }

  ~Shard() { packed_shard_destroy(&_shard); }

  packed_shard_t* operator()() { return &_shard; };
};


int main(int argc, char* argv[])
{
  constexpr size_t n_docs  = 200;
  constexpr size_t n_stats = 42;
  typedef Shard<n_stats> TestShard;

  TestShard::Limits mins, maxes;
  fill(mins.begin(), mins.end(), 0);
  for (size_t i(0); i < maxes.size(); ++i) {
    maxes[i] = 1 << (i % 63);
  }

  TestShard shard(n_docs, mins, maxes,
                  [](size_t doc_id) { return doc_id % 10; },
                  [](int64_t min, int64_t max) { return (min + max) / 2; });

  struct worker_desc worker;
  char *begin(reinterpret_cast<char *>(&worker)), *end(begin + sizeof(worker));
  fill(begin, end, 0);

  vector<uint8_t> slice;
  varint_encode(shard._doc_ids.begin(), shard._doc_ids.end(), slice);

  array<struct index_slice_info, 1> slice_infos({
      { { static_cast<int>(shard._doc_ids.size()), slice.data(), shard() } }
    });
  struct tgs_desc tgs_desc;
  tgs_desc.n_slices        = slice_infos.size();
  tgs_desc.trm_slice_infos = slice_infos.data();

  struct session_desc session;
  session.num_groups       = shard._gids.size();
  session.num_stats        = n_stats;
  session.current_tgs_pass = &tgs_desc;

  tgs_execute_pass(&worker, &session, &tgs_desc);
}
