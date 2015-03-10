#define restrict __restrict__
extern "C" {
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
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <vector>

using namespace std;

template <size_t n_metrics>
struct Shard
{
  typedef Metrics<n_metrics> Metrics;

  packed_table_t *_shard;
  vector<GroupId> _group_ids;

  Shard(const vector<DocId>& doc_ids, size_t n_groups, Metrics& mins, Metrics& maxes)
    : _shard(create_shard_multicache(doc_ids.size(), mins.data(), maxes.data(), n_metrics))
    , _group_ids(doc_ids.size()) {

    transform(doc_ids.begin(), doc_ids.end(), _group_ids.begin(),
              [n_groups](DocId doc_id) { return doc_id % n_groups; });
    packed_table_batch_set_group(_shard, const_cast<DocId*>(doc_ids.data()), doc_ids.size(), _group_ids.data());
  }

  ~Shard() { packed_table_destroy(_shard); }

        packed_table_t * operator()()       { return _shard; };
  const packed_table_t * operator()() const { return _shard; };

};

int main(int argc, char* argv[])
{
  int status(EXIT_SUCCESS);

  size_t n_docs(2048);
  size_t n_groups(100);

  if (argc != 3) {
    //    cerr << "usage: " << argv[0] << " <n_docs> <n_groups>" << endl;
    cerr << "defaulting to n_docs: " << n_docs
         << " n_groups: "            << n_groups
         << endl;
  }
  else {
    n_docs       = atoi(argv[1]);
    n_groups     = atoi(argv[2]);
  }

  static constexpr size_t n_metrics = 5;

  Metrics<n_metrics> mins, maxes;
  fill(mins.begin(),  mins.end(),  0);
  fill(maxes.begin(), maxes.end(), 0xefff);

  vector<DocId> doc_ids(n_docs);
  DocId current(0);
  transform(doc_ids.begin(), doc_ids.end(), doc_ids.begin(),
            [&current](DocId unused) { return current++; });

  Shard<n_metrics> shard(doc_ids, n_groups, mins, maxes);

  vector<GroupId> results(shard._group_ids);
  vector<GroupId> remappings(n_groups);
  for (size_t index(0); index < remappings.size(); ++ index) {
    //    remappings[index] = index % 3;
    remappings[index] = 0;
  }

  vector<GroupId> before(results);
  // cout << "before: " << before << endl;

  vector<uint8_t> buffer;
  vector<DocId> test_ids;
  for (vector<DocId>::const_iterator it(doc_ids.begin()); it < doc_ids.end(); it += 127) {
    test_ids.push_back(*it);
  }
  // doc_ids_encode(doc_ids.begin(), doc_ids.end(), buffer);
  // remap_docs_in_target_groups(shard(), results.data(), buffer.data(), doc_ids.size(), remappings.data(), -1);
  doc_ids_encode(test_ids.begin(), test_ids.end(), buffer);
  remap_docs_in_target_groups(shard(), results.data(), buffer.data(), test_ids.size(), remappings.data(), -1);

  /*
  vector<DocId>::iterator batch_begin(doc_ids.begin());
  while (batch_begin != doc_ids.end()) {
    const size_t            batch_size(min(long(1024), distance(batch_begin, doc_ids.end())));
    vector<DocId>::iterator batch_end(batch_begin + batch_size);
    vector<uint8_t>         buffer;
    doc_ids_encode(batch_begin, batch_end, buffer);
    remap_docs_in_target_groups(shard(), results.data(), buffer.data(), batch_size, remappings.data(), -1);
    batch_begin = batch_end;
  }
  */

  vector<GroupId> after(results);
  // cout << "after: " << after << endl;
  cout << (equal(before.begin(), before.end(), after.begin()) ? "FAIL" : "PASS") << endl;

  return status;
}
