#include <cstdlib>
#include <cstddef>
#include <functional>
#include <iomanip>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <thread>
#include <vector>

#include <boost/asio.hpp>

#define restrict __restrict__
extern "C" {
#include "circ_buf.h"
#include "imhotep_native.h"
#include "local_session.h"
}
#include "test_utils.h"
#include "varintdecode.h"

using boost::asio::ip::tcp;
using namespace std;

typedef function<Metric(size_t index)>             MinMaxFunc;
typedef function<DocId(size_t index)>              DocIdFunc;
typedef function<GroupId(size_t doc_id)>           GroupIdFunc;
typedef function<Metric(int64_t min, int64_t max, size_t metric_num, size_t row_num)> MetricFunc;

typedef vector<int>  DocIds;
typedef set<int64_t> GroupIds;

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
  typedef Metrics<n_metrics> metrics_t;
  const DocId                doc_id;
  const GroupId              group_id;
  metrics_t                  metrics;

  Entry(DocId doc_id_, GroupId group_id_)
    : doc_id(doc_id_)
    , group_id(group_id_)
  { }
};

template <size_t n_metrics>
class StorageFactory
{
  typedef Entry<n_metrics> entry_t;
  typedef vector<entry_t>    Storage;
  typedef map<size_t, shared_ptr<Storage>> Cache;
  Cache _cache;
 public:
  Storage& get(size_t n_docs) {
    typename Cache::iterator it(_cache.find(n_docs));
    if (it == _cache.end()) {
      shared_ptr<Storage> storage(new Storage());
      storage->reserve(n_docs);
      it = _cache.insert(make_pair(n_docs, storage)).first;
    }
    it->second->clear();
    return *(it->second);
  }
};

template <size_t n_metrics>
class Table
{
  typedef vector<Entry<n_metrics>>   Storage;
  typedef Metrics<n_metrics>         metrics_t;
  typedef Entry<n_metrics>           entry_t;
  typedef multimap<GroupId, entry_t> EntriesByGroup;

  static StorageFactory<n_metrics> _storage_factory;

  Storage& _storage;

  metrics_t _mins;
  metrics_t _maxes;

 public:
  Table(size_t             n_docs,
        const MinMaxFunc&  min_func,
        const MinMaxFunc&  max_func,
        const DocIdFunc&   doc_id_func,
        const GroupIdFunc& group_id_func,
        const MetricFunc&  metric_func)
    : _storage(_storage_factory.get(n_docs)) {

    for (size_t metric_index(0); metric_index < n_metrics; ++metric_index) {
      _mins[metric_index]  = min_func(metric_index);
      _maxes[metric_index] = max_func(metric_index);
    }

    for (size_t doc_index(0); doc_index < n_docs; ++doc_index) {
      DocId   doc_id(doc_id_func(doc_index));
      GroupId group_id(group_id_func(doc_id));
      entry_t   entry(doc_id, group_id);
      for (size_t metric_index(0); metric_index < n_metrics; ++metric_index) {
        Metric value(metric_func(_mins[metric_index], _maxes[metric_index], metric_index, doc_index));
        value = min(_mins[metric_index], value);
        value = max(_maxes[metric_index], value);
        entry.metrics[metric_index] = value;
      }
      _storage.push_back(entry);
    }
  }

  size_t size() const { return _storage.size(); }

  const metrics_t& mins()  const { return _mins;  }
  const metrics_t& maxes() const { return _maxes; }

  metrics_t& mins()  { return _mins;  }
  metrics_t& maxes() { return _maxes; }

  DocIds doc_ids() const {
    DocIds result;
    transform(_storage.begin(), _storage.end(), back_inserter(result),
              [](const entry_t& entry) { return entry.doc_id; });
    return result;
  }

  vector<GroupId> flat_group_ids() const {
    vector<GroupId> result;
    transform(_storage.begin(), _storage.end(), back_inserter(result),
              [](const entry_t& entry) { return entry.group_id; });
    return result;
  }

  GroupIds group_ids() const {
    GroupIds result;
    transform(_storage.begin(), _storage.end(), inserter(result, result.begin()),
              [](const entry_t& entry) { return entry.group_id; });
    return result;
  }

  EntriesByGroup entries_by_group() const {
    EntriesByGroup result;
    for (auto entry: _storage) {
      result.insert(make_pair(entry.group_id, entry));
    }
    return result;
  }

  vector<Metric> metrics(size_t metric_index) const {
    vector<Metric> result;
    transform(_storage.begin(), _storage.end(), inserter(result, result.begin()),
              [metric_index, &result](const entry_t& entry) { return entry.metrics[metric_index]; });
    return result;
  }

  vector<metrics_t> metrics() const {
    vector<metrics_t> result;
    for_each(_storage.begin(), _storage.end(),
             [&result](const entry_t& entry) { result.push_back(entry.metrics); });
    return result;
  }

  GroupStats<n_metrics> sum() const {
    GroupStats<n_metrics> result;
    const GroupIds       group_ids(this->group_ids());
    const EntriesByGroup entries(entries_by_group());
    for (auto group_id: group_ids) {
      metrics_t row;
      auto range(entries.equal_range(group_id));
      for_each(range.first, range.second,
               [&] (const typename EntriesByGroup::value_type& value) {
                 for (size_t metric_index(0); metric_index < n_metrics; ++metric_index) {
                     row[metric_index] += value.second.metrics[metric_index];
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

template <size_t n_metrics> StorageFactory<n_metrics> Table<n_metrics>::_storage_factory;

template <size_t n_metrics>
struct Shard
{
  const Table<n_metrics>& _table;
  ShardAttrs<n_metrics>   _attrs;
  packed_table_t*         _shard;

  Shard(const Table<n_metrics>& table)
    : _table(table)
    , _attrs(_table.mins(), _table.maxes())
    , _shard(create_shard_multicache(table.size(),
                                     const_cast<int64_t*>(table.mins().data()),
                                     const_cast<int64_t*>(table.maxes().data()),
                                     _attrs.sizes, _attrs.vec_nums, _attrs.offsets_in_vecs,
                                     vector<int8_t>(n_metrics, 0).data(), // !@# currently unused
                                     n_metrics)) {

    DocIds          doc_ids(table.doc_ids());
    vector<GroupId> flat_group_ids(table.flat_group_ids());
    packed_table_batch_set_group(_shard, doc_ids.data(), doc_ids.size(), flat_group_ids.data());

    for (size_t metric_index(0); metric_index < n_metrics; ++metric_index) {
      vector<Metric> metrics(table.metrics(metric_index));
      packed_table_batch_set_col(_shard, doc_ids.data(), doc_ids.size(),
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


void echo_stats(unsigned short port)
{
  boost::asio::io_service ios;
  tcp::acceptor           acc(ios, tcp::endpoint(tcp::v4(), port));
  tcp::socket             sock(ios);
  acc.accept(sock);

  char stats[4096];
  boost::system::error_code error;
  while (true) {
    size_t length(sock.read_some(boost::asio::buffer(stats), error));
    if (error == boost::asio::error::eof) {
      break;
    }
    else if (error) {
      throw boost::system::system_error(error);
    }
    cerr << "read: " << length << endl;
    vector<uint8_t> out;
    copy_n(stats, length, back_inserter(out));
    cerr << out << endl;
  }
}

static unsigned short test_port = 12345;

template <size_t n_metrics>
class TGSTest
{
  const size_t _n_docs;
  const size_t _n_groups;

  const Table<n_metrics> _table;

  Shard<n_metrics>    _shard;
  struct worker_desc  _worker;
  struct session_desc _session;

  thread _echo_thread;

public:
  typedef Shard<n_metrics>      shard_t;
  typedef GroupStats<n_metrics> group_stats_t;

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
    , _shard(_table)
    , _echo_thread(echo_stats, ++test_port) {

    this_thread::sleep_for(chrono::seconds(3));

    boost::asio::io_service io_service;
    tcp::socket             sock(io_service);
    tcp::resolver           resolver(io_service);
    boost::asio::connect(sock, resolver.resolve({"localhost", port_string(test_port)}));

    array <int, 1> socket_file_desc{{sock.native_handle()}};
    worker_init(&_worker, 1, n_groups, n_metrics, socket_file_desc.data(), 1);

    packed_table_t* shard_order[] = { _shard() };
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
                 TERM_TYPE_INT, 1,
                 NULL, 0,
                 addresses.data(),
                 docs_in_term.data(),
                 1, 0);

    this_thread::sleep_for(chrono::seconds(3));
  }

  ~TGSTest() {
    session_destroy(&_session);
    worker_destroy(&_worker);
    _echo_thread.join();
  }

  const size_t   n_docs() const { return _n_docs;   }
  const size_t n_groups() const { return _n_groups; }

  const Table<n_metrics>&           table() const { return _table;              }
  const shard_t&                    shard() const { return _shard;              }
  unpacked_table_t*       group_stats_buf() const { return _worker.grp_stats;   }

private:
  string port_string(unsigned short port) {
    ostringstream os;
    os << port;
    return os.str();
  }
};


template <size_t n_metrics>
ostream& operator<<(ostream& os, const TGSTest<n_metrics>& test) {
  typedef TGSTest<n_metrics> Test;
  const typename Test::group_stats_t thing1(test.table().sum());
  cout << "thing1: " << thing1 << endl;
  const typename Test::group_stats_t thing2(test.shard().sum(test.group_stats_buf()));
  cout << "thing2: " << thing2 << endl;
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

  simdvbyteinit();

  vector<MetricFunc> metric_funcs = {
    /*
    [](int64_t min_val, int64_t max_val, size_t metric_num, size_t row_num) { return min_val; },
    [](int64_t min_val, int64_t max_val, size_t metric_num, size_t row_num) { return max_val; },
    [](int64_t min_val, int64_t max_val, size_t metric_num, size_t row_num) { return min_val + (max_val - min_val) / 2; },
    [](int64_t min_val, int64_t max_val, size_t metric_num, size_t row_num) {
        int64_t val = metric_num * row_num; // !@# check for overflow
        if (val > max_val)
            return max_val;
        if (val < min_val)
            return min_val;
        return (long)val;
    },
    */
    [](int64_t min_val, int64_t max_val, size_t metric_num, size_t row_num) { return (max_val == min_val)
            ? max_val
            : min_val + (rand() % (max_val - min_val)); },
    /*
    [](int64_t min_val, int64_t max_val, size_t metric_num, size_t row_num) { return 1 << metric_num; },
    */
  };

  vector<pair<MinMaxFunc, MinMaxFunc>> min_max_funcs = {
    /*
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
    */
    make_pair([](size_t index) { return 1; },
              [](size_t index) { return 1 << 30; }),
    /*
    make_pair([](size_t index) { return -(1 << 30); },
            [](size_t index) { return 1 << 30; }),
    */
  };

  for (auto metric_func: metric_funcs) {
    for (auto min_max_func: min_max_funcs) {
      const MinMaxFunc  min_func(min_max_func.first);
      const MinMaxFunc  max_func(min_max_func.second);
      const DocIdFunc   doc_id_func([](size_t index) { return index; });
      const GroupIdFunc group_id_func([n_groups](size_t doc_id) { return doc_id % n_groups; });

      /*
      TGSTest<1>   test1(n_docs, n_groups, min_func, max_func, doc_id_func, group_id_func, metric_func);
      cout << test1;
      TGSTest<2>   test2(n_docs, n_groups, min_func, max_func, doc_id_func, group_id_func, metric_func);
      cout << test2;
      TGSTest<5>   test5(n_docs, n_groups, min_func, max_func, doc_id_func, group_id_func, metric_func);
      cout << test5;
      TGSTest<20> test20(n_docs, n_groups, min_func, max_func, doc_id_func, group_id_func, metric_func);
      cout << test20;
      */
      for (size_t count(0); count < 10; ++count) {
        TGSTest<64> test64(n_docs, n_groups, min_func, max_func, doc_id_func, group_id_func, metric_func);
        cout << test64;
      }
    }
  }

  return status;
}
