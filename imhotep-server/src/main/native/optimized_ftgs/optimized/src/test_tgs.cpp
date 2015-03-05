#define restrict __restrict__
extern "C" {
#include "circ_buf.h" 
#include "imhotep_native.h"
#include "local_session.h"
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
#include <sstream>
#include <vector>

using namespace std;

typedef int     DocId;
typedef int64_t GroupId;
typedef int64_t Metric;

typedef function<Metric(size_t index)>             MinMaxFunc;
typedef function<DocId(size_t index)>              DocIdFunc;
typedef function<GroupId(size_t doc_id)>           GroupIdFunc;
typedef function<Metric(int64_t min, int64_t max, size_t metric_num, size_t row_num)> MetricFunc;

typedef vector<int>  DocIds;
typedef set<int64_t> GroupIds;


template <size_t n_metrics>
struct Metrics : public array<Metric, n_metrics>
{
  Metrics() { fill(this->begin(), this->end(), 0); }
};

template <size_t n_metrics>
ostream& operator<<(ostream& os, const Metrics<n_metrics>& row) {
  for (auto element: row) os << element << " ";
  return os;
}

template <size_t n_metrics>
ostream& operator<<(ostream& os, const vector<Metrics<n_metrics>>& rows) {
  for (auto row: rows) os << row << endl;
  return os;
}


template <size_t n_metrics>
struct GroupStats : public map<GroupId, Metrics<n_metrics>>
{ };

template <size_t n_metrics>
ostream& operator<<(ostream& os, GroupStats<n_metrics> stats) {
  for (auto row: stats)
    os << "gid " << setw(3) << row.first << ": " << row.second << endl;
  return os;
}


template <size_t n_metrics>
class Entry
{
public:
  typedef Metrics<n_metrics> Metrics;
  const DocId   doc_id;
  const GroupId group_id;
  Metrics       metrics;

  Entry(DocId doc_id_, GroupId group_id_)
    : doc_id(doc_id_)
    , group_id(group_id_)
  { }
};


template <size_t n_metrics>
class Table : public vector<Entry<n_metrics>>
{
  typedef Metrics<n_metrics>       Metrics;
  typedef Entry<n_metrics>         Entry;
  typedef multimap<GroupId, Entry> EntriesByGroup;

  Metrics _mins;
  Metrics _maxes;

 public:
  Table(size_t             n_docs,
        const MinMaxFunc&  min_func,
        const MinMaxFunc&  max_func,
        const DocIdFunc&   doc_id_func,
        const GroupIdFunc& group_id_func,
        const MetricFunc&  metric_func) {

    for (size_t metric_index(0); metric_index < n_metrics; ++metric_index) {
      _mins[metric_index]  = min_func(metric_index);
      _maxes[metric_index] = max_func(metric_index);
    }      

    for (size_t doc_index(0); doc_index < n_docs; ++doc_index) {
      DocId   doc_id(doc_id_func(doc_index));
      GroupId group_id(group_id_func(doc_id));
      Entry   entry(doc_id, group_id);
      for (size_t metric_index(0); metric_index < n_metrics; ++metric_index) {
        Metric value(metric_func(_mins[metric_index], _maxes[metric_index]));
        value = min(_mins[metric_index], value);
        value = max(_maxes[metric_index], value);
        entry.metrics[metric_index] = value;
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

  GroupStats<n_metrics> sum() const {
    GroupStats<n_metrics> result;
    const GroupIds       group_ids(this->group_ids());
    const EntriesByGroup entries(entries_by_group());
    for (auto group_id: group_ids) {
      Metrics row;
      auto range(entries.equal_range(group_id));
      for_each(range.first, range.second,
               [&] (const typename EntriesByGroup::value_type& value) {
                 for (size_t metric_index(0); metric_index < n_metrics; ++metric_index) {
//                   if (!is_boolean(metric_index)) {
                     row[metric_index] += value.second.metrics[metric_index];
//                   } else {
//                     row[metric_index] = max(row[metric_index], value.second.metrics[metric_index]);
//                   }
                 }
               });
      result.insert(make_pair(group_id, row));
    }
    return result;
  }

 private:
  bool is_boolean(size_t metric_index) const {
    return _maxes[metric_index] - _mins[metric_index] == 1;
  }
};


template <size_t n_metrics>
struct Shard
{
  const Table<n_metrics>&  _table;
  packed_table_t          *_shard;

  Shard(const Table<n_metrics>& table)
    : _table(table)
    , _shard(create_shard_multicache(table.size(), table.mins().data(), table.maxes().data(), n_metrics)) {

    DocIds          doc_ids(table.doc_ids());
    vector<GroupId> flat_group_ids(table.flat_group_ids());
    packed_shard_batch_set_group(_shard, doc_ids.data(), doc_ids.size(), flat_group_ids.data());

    for (size_t metric_index(0); metric_index < n_metrics; ++metric_index) {
      vector<Metric> metrics(table.metrics(metric_index));
      packed_shard_batch_set_col(_shard, doc_ids.data(), doc_ids.size(),
                                 metrics.data(), metric_index);
    }
  }

  ~Shard() { packed_table_destroy(_shard); }

        packed_table_t * operator()()       { return _shard; };
  const packed_table_t * operator()() const { return _shard; };

  GroupStats<n_metrics> sum(unpacked_table_t* group_stats_buf) const {
    GroupStats<n_metrics> results;
    GroupIds              gids(_table.group_ids());
    size_t                row_index(0);
    for (GroupIds::const_iterator it(gids.begin()); it != gids.end(); ++it, ++row_index) {
      Metrics<n_metrics> row;
      for (int col_index(0); col_index < unpacked_table_get_cols(group_stats_buf); ++col_index) {
        row[col_index] = unpacked_table_get_cell(group_stats_buf, row_index, col_index);
      }
      results.insert(make_pair(*it, row));
    }
    return results;
  }

};


template <size_t n_metrics>
class TGSTest
{
  const size_t _n_docs;
  const size_t _n_groups;

  const Table<n_metrics> _table;

  Shard<n_metrics>    _shard;
  struct worker_desc  _worker;
  struct session_desc _session;

public:
  typedef Metrics<n_metrics>    Metrics;
  typedef Shard<n_metrics>      Shard;
  typedef GroupStats<n_metrics> GroupStats;

  TGSTest(size_t             n_docs,
          size_t             n_groups,
          const MinMaxFunc&  min_func,
          const MinMaxFunc&  max_func,
          const DocIdFunc&   doc_id_func,
          const GroupIdFunc& group_id_func,
          const MetricFunc&  metric_func)
    : _n_docs(n_docs)
    , _n_groups(n_groups)
    , _table(n_docs, min_func, max_func, doc_id_func, group_id_func, metric_func)
    , _shard(_table) {

    array <int, 1> socket_file_desc{{3}};
    worker_init(&_worker, 1, n_groups, n_metrics, socket_file_desc.data(), 1);

    uint8_t shard_order[] = {0};
    session_init(&_session, n_groups, n_metrics, shard_order, 1);

    array <int, 1> shard_handles;
    shard_handles[0] = register_shard(&_session, _shard());

    DocIds doc_ids(_table.doc_ids());
    vector<uint8_t> slice;
    doc_ids_encode(doc_ids.begin(), doc_ids.end(), slice);
    array<long, 1> addresses{{reinterpret_cast<long>(slice.data())}};

    array<int, 1> docs_in_term{{static_cast<int>(_table.doc_ids().size())}};

    run_tgs_pass(&_worker,
                 &_session,
                 TERM_TYPE_INT,
                 1,
                 NULL,
                 addresses.data(),
                 docs_in_term.data(),
                 shard_handles.data(),
                 1,
                 socket_file_desc[0]);

  }

  ~TGSTest() {
    session_destroy(&_session);
    worker_destroy(&_worker);
  }

  const size_t   n_docs() const { return _n_docs;   }
  const size_t n_groups() const { return _n_groups; }

  const Table<n_metrics>&           table() const { return _table;              }
  const Shard&                      shard() const { return _shard;              }
  unpacked_table_t*       group_stats_buf() const { return _worker.grp_stats;   }
};

template <size_t n_metrics>
ostream& operator<<(ostream& os, const TGSTest<n_metrics>& test) {
  typedef TGSTest<n_metrics> Test;
  const typename Test::GroupStats thing1(test.table().sum());
  const typename Test::GroupStats thing2(test.shard().sum(test.group_stats_buf()));
  stringstream description;
  description << "n_metrics: " << n_metrics
              << " n_docs: "   << test.n_docs()
              << " n_groups: " << test.n_groups();
  if (thing1 != thing2) {
    cout << "FAILED " << description.str() << endl;
    cerr << "expected:" << endl << thing1 << endl;
    cerr << "actual:"   << endl << thing2 << endl;
    // cout << "_table:" << endl;
    // cout << _table.metrics() << endl << endl;
  }
  else {
    cout << "PASSED " << description.str() << endl;
  }
  return os;
}

int main(int argc, char* argv[])
{
  int status(EXIT_SUCCESS);

  size_t n_docs(1000);
  size_t n_groups(100);

  if (argc != 3) {
    //    cerr << "usage: " << argv[0] << " <n_docs> <n_groups>" << endl;
    cerr << "defaulting to n_docs: " << n_docs << " n_groups: " << n_groups << endl;
  }
  else {
    n_docs   = atoi(argv[1]);
    n_groups = atoi(argv[2]);
  }

  vector<MetricFunc> metric_funcs = {
    [](int64_t min_val, int64_t max_val, size_t metric_num, size_t row_num) { return min_val; },
    [](int64_t min_val, int64_t max_val, size_t metric_num, size_t row_num) { return max_val; },
    [](int64_t min_val, int64_t max_val, size_t metric_num, size_t row_num) { return min_val + (max_val - min_val) / 2; },
    [](int64_t min_val, int64_t max_val, size_t metric_num, size_t row_num) {
        auto val = metric_num * row_num;
        if (val > max_val)
            return max_val;
        if (val < min_val)
            return min_val;
        return (long)val;
    },
    [](int64_t min_val, int64_t max_val, size_t metric_num, size_t row_num) { return (max_val == min_val)
            ? max_val
            : min_val + (rand() % (max_val - min_val)); },
//    [](int64_t min_val, int64_t max_val, size_t metric_num, size_t row_num) { return 1 << metric_num; },
  };

  vector<pair<MinMaxFunc, MinMaxFunc>> min_max_funcs = {
    make_pair([](size_t index) { return 0; },
              [](size_t index) { return 0; }),
    make_pair([](size_t index) { return 0; },
              [](size_t index) { return 1; }),
    make_pair([](size_t index) { return 0; },
              [](size_t index) { return 32; }),
    make_pair([](size_t index) { return 0; },
              [](size_t index) { return 1 << 30; }),
    make_pair([](size_t index) { return 1; },
              [](size_t index) { return 1; }),
    make_pair([](size_t index) { return 1; },
              [](size_t index) { return 2; }),
    make_pair([](size_t index) { return 1; },
              [](size_t index) { return 32; }),
    make_pair([](size_t index) { return 1; },
              [](size_t index) { return 1 << 30; }),
    make_pair([](size_t index) { return -(1 << 30); },
            [](size_t index) { return 1 << 30; }),
  };

  for (auto metric_func: metric_funcs) {
    for (auto min_max_func: min_max_funcs) {
      const MinMaxFunc  min_func(min_max_func.first);
      const MinMaxFunc  max_func(min_max_func.second);
      const DocIdFunc   doc_id_func([](size_t index) { return index; });
      const GroupIdFunc group_id_func([n_groups](size_t doc_id) { return doc_id % n_groups; });

      TGSTest<1>   test1(n_docs, n_groups, min_func, max_func, doc_id_func, group_id_func, metric_func);
      cout << test1;
      TGSTest<2>   test2(n_docs, n_groups, min_func, max_func, doc_id_func, group_id_func, metric_func);
      cout << test2;
      TGSTest<5>   test5(n_docs, n_groups, min_func, max_func, doc_id_func, group_id_func, metric_func);
      cout << test5;
      TGSTest<20> test20(n_docs, n_groups, min_func, max_func, doc_id_func, group_id_func, metric_func);
      cout << test20;
//      TGSTest<64> test64(n_docs, n_groups, min_func, max_func, doc_id_func, group_id_func, metric_func);
//      cout << test64;
    }
  }

  return status;
}
