#define restrict __restrict__
extern "C" {
#include "imhotep_native.h"
}

#include <algorithm>
#include <array>
#include <iomanip>
#include <iostream>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

using namespace std;

template <class T, size_t N>
ostream& operator<<(ostream& os, const array<T, N>& items)
{
  for (typename array<T, N>::const_iterator it(items.begin()); it != items.end(); ++it) {
    if (it != items.begin()) os << " ";
    os << *it;
  }
  return os;
}

template <size_t n_docs, size_t n_metrics>
void test_packed_shards(array<int64_t, n_metrics>& mins,
                        array<int64_t, n_metrics>& maxes)
{
  bool passed(true);

	packed_shard_t shard;
	packed_shard_init(&shard, n_docs, mins.data(), maxes.data(), n_metrics);
		
  for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
		int64_t group = doc_id;
		packed_shard_update_groups(&shard, &doc_id, 1, &group);
  }

	for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
		int64_t expected = doc_id;
		int64_t actual[1] = { -1 }; // change to array
		packed_shard_lookup_groups(&shard, &doc_id, 1, actual);
		if (expected != actual[0]) {
      cerr << "group lookup failed -- expected: " << expected << " actual: " << actual[0] << endl;
      passed = false;
		}
	}

	for (int metric_index = 0; metric_index < n_metrics; ++metric_index) {
    array<int64_t, n_metrics> metrics;
		for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
			metrics[metric_index] = mins[metric_index] + doc_id;
			packed_shard_update_metric(&shard, &doc_id, 1, &metrics[metric_index], metric_index);
		}
	}

	for (int metric_index = 0; metric_index < n_metrics; ++metric_index) {
    array<int64_t, n_metrics> metrics;
		for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
			int64_t expected = mins[metric_index] + doc_id;
			packed_shard_lookup_metric_values(&shard, &doc_id, 1, &metrics[metric_index], metric_index);
			if (metrics[metric_index] != expected) {
        cerr << "metric mismatch -- doc_id: " << doc_id << " metric_index: " << metric_index
             << " expected: " << expected << " actual: " << metrics[metric_index] << endl;
        passed = false;
      }
		}
	}
	packed_shard_destroy(&shard);

  cout << (passed ? "PASSED" : "FAILED")
       << " n_docs: "    << setw(10) << left << n_docs
       << " n_metrics: " << setw(10) << left << n_metrics << " "
       << endl;

  if (!passed) {
    cout << " mins: " << mins << endl;
    cout << "maxes: " << maxes << endl;
  }
}

template <size_t n_docs, size_t n_metrics, int64_t min_value, int64_t max_value>
void test_uniform()
{
  array<int64_t, n_metrics> mins;  fill(mins.begin(),  mins.end(),  min_value);
  array<int64_t, n_metrics> maxes; fill(maxes.begin(), maxes.end(), max_value);
  test_packed_shards<n_docs, n_metrics>(mins, maxes);
}

int main(int argc, char * argv[])
{
  test_uniform<1,  1,  0, 1>();
  test_uniform<1,  2,  0, 1>();
  test_uniform<1,  64, 0, 1>();
  test_uniform<2,  1,  0, 1>();
  test_uniform<2,  2,  0, 1>();
  test_uniform<2,  64, 0, 1>();
  test_uniform<99, 1,  0, 1>();
  test_uniform<99, 2,  0, 1>();
  test_uniform<99, 64, 0, 1>();

  test_uniform<1,  1,  0, 0x0f>();
  test_uniform<1,  2,  0, 0x0f>();
  test_uniform<1,  64, 0, 0x0f>();
  test_uniform<2,  1,  0, 0x0f>();
  test_uniform<2,  2,  0, 0x0f>();
  test_uniform<2,  64, 0, 0x0f>();
  test_uniform<99, 1,  0, 0x0f>();
  test_uniform<99, 2,  0, 0x0f>();
  test_uniform<99, 64, 0, 0x0f>();
}

