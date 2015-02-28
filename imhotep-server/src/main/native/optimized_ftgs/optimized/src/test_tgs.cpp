#define restrict __restrict__
extern "C" {
#include "circ_buf.h" 
#include "imhotep_native.h"
}

#include "test_utils.h"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <vector>

using namespace std;

typedef int     DocId;
typedef int64_t GroupId;
typedef int64_t Metric;

typedef vector<int>  DocIds;
typedef set<int64_t> GroupIds;

template <size_t n_metrics>
struct Metrics : public array<Metric, n_metrics>
{
  Metrics() { fill(this->begin(), this->end(), 0); } // !@# redundant?
};

template <size_t n_metrics>
struct Entry {
  typedef Metrics<n_metrics> Metrics;
  DocId   doc_id;
  Metrics metrics;
  GroupId group_id;

  Entry(DocId doc_id_, GroupId group_id_)
    : doc_id(doc_id_)
    , group_id(group_id_) {
    fill(metrics.begin(), metrics.end(), 0);
  }
};

typedef function<int64_t(size_t index)> DocIdFunc;
typedef function<int64_t(size_t doc_id)> GroupIdFunc;
typedef function<int64_t(int64_t min, int64_t max)> MetricFunc;

template <size_t n_metrics>
class Table : public vector<Entry<n_metrics>> {

  typedef Metrics<n_metrics>       Metrics;
  typedef Entry<n_metrics>         Entry;
  typedef multimap<GroupId, Entry> EntriesByGroup;

  const Metrics _mins;
  const Metrics _maxes;

 public:
  Table(size_t             n_docs,
        const Metrics&     mins,
        const Metrics&     maxes,
        const DocIdFunc&   doc_id_func,
        const GroupIdFunc& group_id_func,
        const MetricFunc&  metric_func) 
    : _mins(mins)
    , _maxes(maxes) {
    for (size_t doc_index(0); doc_index < n_docs; ++doc_index) {
      DocId   doc_id(doc_id_func(doc_index));
      GroupId group_id(group_id_func(doc_id));
      Entry   entry(doc_id, group_id);
      for (size_t metric_index(0); metric_index < n_metrics; ++metric_index) {
        entry.metrics[metric_index] = metric_func(mins[metric_index], maxes[metric_index]);
      }
      this->push_back(entry);
    }
  }

  Metrics mins()  const { return _mins;  }
  Metrics maxes() const { return _maxes; }

  DocIds doc_ids() const {
    DocIds result;
    transform(this->begin(), this->end(), back_inserter(result),
              [](const Entry& entry) { return entry.doc_id; });
    return result;
  }

  vector<GroupId> flat_group_ids() const {
    vector<GroupId> result;
    transform(this->begin(), this->end(), back_inserter(result),
              [](const Entry& entry) { return entry.group_id; });
    return result;
  }

  GroupIds group_ids() const {
    GroupIds result;
    transform(this->begin(), this->end(), inserter(result, result.begin()),
              [](const Entry& entry) { return entry.group_id; });
    return result;
  }

  EntriesByGroup entries_by_group() const {
    EntriesByGroup result;
    for (auto entry: *this) {
      result.insert(make_pair(entry.group_id, entry));
    }
    return result;
  }

  vector<Metric> metrics(size_t metric_index) const {
    vector<Metric> result;
    transform(this->begin(), this->end(), inserter(result, result.begin()),
              [metric_index, &result](const Entry& entry) { return entry.metrics[metric_index]; });
    return result;
  }

  vector<Metrics> metrics() const {
    vector<Metrics> result;
    for_each(this->begin(), this->end(),
             [&result](const Entry& entry) { result.push_back(entry.metrics); });
    return result;
  }

  Metrics sum(GroupId group_id) const {
    Metrics result;
    for (auto entry: *this) {
      if (entry.group_id == group_id) {
        for (size_t index(0); index < entry.metrics.size(); ++index) {
          result[index] += entry.metrics[index];
        }
      }
    }
    return result;
  }

  vector<Metrics> sum() const {
    vector<Metrics> result;
    const GroupIds       group_ids(this->group_ids());
    const EntriesByGroup entries(entries_by_group());
    for (auto group_id: group_ids) {
      Metrics row;
      auto range(entries.equal_range(group_id));
      for_each(range.first, range.second,
               [&row] (const typename EntriesByGroup::value_type& value) {
                 for (size_t metric_index(0); metric_index < n_metrics; ++metric_index) {
                   row[metric_index] += value.second.metrics[metric_index];
                 }
               });
      result.push_back(row);
    }
    return result;
  }
};

template <size_t n_metrics>
ostream& operator<<(ostream& os, const vector<Metrics<n_metrics>>& rows) {
  for (auto row: rows) {
    for (auto element: row) {
      os << setw(10) << element;
    }
    os << endl;
  }
  return os;
}

template <size_t n_metrics>
struct Shard {

  packed_shard_t _shard;

  Shard(const Table<n_metrics>& table) {

    packed_shard_init(&_shard, table.size(), table.mins().data(), table.maxes().data(), n_metrics);

    DocIds          doc_ids(table.doc_ids());
    vector<GroupId> flat_group_ids(table.flat_group_ids());
    packed_shard_update_groups(&_shard, doc_ids.data(), doc_ids.size(), flat_group_ids.data());

    for (size_t metric_index(0); metric_index < n_metrics; ++metric_index) {
      vector<Metric> metrics(table.metrics(metric_index));
      packed_shard_update_metric(&_shard, doc_ids.data(), doc_ids.size(),
                                 metrics.data(), metric_index);
    }
  }

  ~Shard() { packed_shard_destroy(&_shard); }

  packed_shard_t* operator()() { return &_shard; };
};


int main(int argc, char* argv[])
{
  constexpr size_t circ_buf_size = 32; // !@#
  constexpr size_t n_docs  = 32;
  constexpr size_t n_metrics = 13;
  typedef Shard<n_metrics> TestShard;

  int status(EXIT_SUCCESS);

  Metrics<n_metrics> mins, maxes;
  fill(mins.begin(), mins.end(), 0);
  fill(maxes.begin(), maxes.end(), 13);
  maxes[0] = 1;
  maxes[1] = 1;
  maxes[2] = 1;
  maxes[3] = 1;

  Table<n_metrics> table(n_docs, mins, maxes, 
                         [](size_t index) { return index; },
                         [](size_t doc_id) { return doc_id % 4; }, // i.e. group_id == doc_id
                         [](int64_t min, int64_t max) { /*return (max - min) / 2;*/ return max; });

  cout << table.metrics() << endl << endl;
  cout << table.sum() << endl;

  TestShard shard(table);

  struct bit_tree bit_tree;
  //  bit_tree_init(&bit_tree, 1024); // !@# arbitrary size!
  bit_tree_init(&bit_tree, 4096); // !@# arbitrary size!

  struct worker_desc worker;
  char *begin(reinterpret_cast<char *>(&worker)), *end(begin + sizeof(worker));
  fill(begin, end, 0);
  worker.bit_tree_buf = &bit_tree;
	worker.grp_buf      = circular_buffer_int_alloc(circ_buf_size);
	worker.metric_buf   = circular_buffer_vector_alloc((n_metrics+1)/2 * circ_buf_size);
//  worker.metric_buf   = reinterpret_cast<__m128i*>(aligned_alloc(64, sizeof(uint64_t) * n_metrics * 2));

  DocIds doc_ids(table.doc_ids());
  vector<uint8_t> slice;
  doc_ids_encode(doc_ids.begin(), doc_ids.end(), slice);

  array<struct index_slice_info, 1> slice_infos({
      { { static_cast<int>(doc_ids.size()), slice.data(), shard() } }
    });
  struct tgs_desc tgs_desc;
  tgs_desc.n_slices        = slice_infos.size();
  tgs_desc.trm_slice_infos = slice_infos.data();
	tgs_desc.grp_buf         = worker.grp_buf;
	tgs_desc.metric_buf      = worker.metric_buf;
  tgs_desc.non_zero_groups = worker.bit_tree_buf;

  GroupIds gids(table.group_ids());
  struct session_desc session;
  session.num_groups       = gids.size();
  session.num_stats        = n_metrics;
  session.current_tgs_pass = &tgs_desc;

  tgs_execute_pass(&worker, &session, &tgs_desc);

  typedef array<uint64_t, n_metrics> Row;
  size_t row_index(0);
  for (GroupIds::const_iterator it(gids.begin()); it != gids.end(); ++it, ++row_index) {
    cout << "gid: " << *it << endl;
    char* begin(reinterpret_cast<char*>(worker.group_stats_buf) + row_index * sizeof(Row));
    Row& row(*reinterpret_cast<Row*>(begin));
    cout << row << endl;
  }

  circular_buffer_vector_cleanup(worker.metric_buf);
  circular_buffer_int_cleanup(worker.grp_buf);
  bit_tree_destroy(&bit_tree);

  return status;
}
