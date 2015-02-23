#define restrict __restrict__
extern "C" {
#include "imhotep_native.h"
}

#include <algorithm>
#include <array>
#include <functional>
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

// (doc_id, metric_index, min, max) -> metric value
typedef function<int64_t(int, int, int64_t, int64_t)> MetricFunc;

static MetricFunc
default_metric_func([](int doc_id, int metric_index, int64_t min_val, int64_t max_val) {
                      int64_t result(min_val + doc_id);
                      result = max(min_val, result);
                      result = min(max_val, result);
                      return result;
                    });

template <size_t n_docs, size_t n_metrics>
bool test_packed_shards(array<int64_t, n_metrics>& mins,
                        array<int64_t, n_metrics>& maxes,
                        MetricFunc metric_func)
{
  bool result(true);

	packed_shard_t shard;
	packed_shard_init(&shard, n_docs, mins.data(), maxes.data(), n_metrics);
		
  for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
		int64_t group = doc_id;
		packed_shard_update_groups(&shard, &doc_id, 1, &group);
  }

	for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
		int64_t expected = doc_id;
		int64_t actual(0);
		packed_shard_lookup_groups(&shard, &doc_id, 1, &actual);
		if (expected != actual) {
      cerr << "group lookup failed -- expected: " << expected << " actual: " << actual << endl;
      result = false;
		}
	}

	for (int metric_index = 0; metric_index < n_metrics; ++metric_index) {
    array<int64_t, n_metrics> metrics;
		for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
			metrics[metric_index] = metric_func(doc_id, metric_index, mins[metric_index], maxes[metric_index]);
			packed_shard_update_metric(&shard, &doc_id, 1, &metrics[metric_index], metric_index);
		}
	}

	for (int metric_index = 0; metric_index < n_metrics; ++metric_index) {
    array<int64_t, n_metrics> metrics;
		for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
			int64_t expected = metric_func(doc_id, metric_index, mins[metric_index], maxes[metric_index]);
			packed_shard_lookup_metric_values(&shard, &doc_id, 1, &metrics[metric_index], metric_index);
			if (metrics[metric_index] != expected) {
        cerr << "metric mismatch -- doc_id: " << doc_id << " metric_index: " << metric_index
             << " expected: " << expected << " actual: " << metrics[metric_index] << endl;
        result = false;
      }
		}
	}
	packed_shard_destroy(&shard);

  return result;
}

// (n_docs, n_metrics) -> max/min value
typedef function<int64_t(size_t, int64_t)> MapFunc;

template <size_t n_docs, size_t n_metrics,
          bool should_succeed=true>
void test_func(MapFunc min_func,
               MapFunc max_func,
               MetricFunc metric_func)
{
  array<int64_t, n_metrics> mins;
  array<int64_t, n_metrics> maxes;
  for (size_t i(0); i < n_metrics; ++i) {
    mins[i]  = min_func(n_docs, i);
    maxes[i] = max_func(n_docs, i);
  }
  const bool result(test_packed_shards<n_docs, n_metrics>(mins, maxes, metric_func));
  // cout << " mins: " << mins << endl;
  // cout << "maxes: " << maxes << endl;
  cout << (result == should_succeed ? "PASSED" : "FAILED")
       << " n_docs: "    << setw(10) << left << n_docs
       << " n_metrics: " << setw(10) << left << n_metrics << " "
       << endl;
}

template <size_t n_docs, size_t n_metrics,
          int64_t min_value, int64_t max_value,
          bool should_succeed=true>
void test_uniform(MetricFunc metric_func)
{
  MapFunc min_func([](size_t, size_t) { return min_value; });
  MapFunc max_func([](size_t, size_t) { return max_value; });
  test_func<n_docs, n_metrics, should_succeed>(min_func, max_func, metric_func);
}

int main(int argc, char * argv[])
{
  vector<MetricFunc> metric_funcs = {
    [](int, int, int64_t min_val, int64_t) { return min_val; },
    [](int, int, int64_t, int64_t max_val) { return max_val; },
    [](int doc_id, int metric_index, int64_t min_val, int64_t max_val) {
      int64_t result(min_val + doc_id + metric_index);
      result = max(min_val, result);
      result = min(max_val, result);
      return result;
    },
    [](int doc_id, int metric_index, int64_t min_val, int64_t max_val) {
      int64_t result(max_val - doc_id - metric_index);
      result = max(min_val, result);
      result = min(max_val, result);
      return result;
    }
  };

  for (auto metric_func: metric_funcs) {
    test_uniform<1,  1,  0, 1>(metric_func);
    test_uniform<1,  2,  0, 1>(metric_func);
    test_uniform<1,  64, 0, 1>(metric_func);
    test_uniform<2,  1,  0, 1>(metric_func);
    test_uniform<2,  2,  0, 1>(metric_func);
    test_uniform<2,  64, 0, 1>(metric_func);
    test_uniform<99, 1,  0, 1>(metric_func);
    test_uniform<99, 2,  0, 1>(metric_func);
    test_uniform<99, 64, 0, 1>(metric_func);

    test_uniform<1,  1,  0, 0x0f>(metric_func);
    test_uniform<1,  2,  0, 0x0f>(metric_func);
    test_uniform<1,  64, 0, 0x0f>(metric_func);
    test_uniform<2,  1,  0, 0x0f>(metric_func);
    test_uniform<2,  2,  0, 0x0f>(metric_func);
    test_uniform<2,  64, 0, 0x0f>(metric_func);
    test_uniform<99, 1,  0, 0x0f>(metric_func);
    test_uniform<99, 2,  0, 0x0f>(metric_func);
    // test_uniform<99, 64, 0, 0x0f>(metric_func);

    MapFunc min_func([](size_t n_docs, size_t n_metric) { return n_metric;                              });
    MapFunc max_func([](size_t n_docs, size_t n_metric) { return n_docs * n_docs * n_metric * n_metric; });
  
    test_func<1,    1>(min_func, max_func, metric_func);
    test_func<1,    2>(min_func, max_func, metric_func);
    test_func<1,   64>(min_func, max_func, metric_func);
    test_func<2,    1>(min_func, max_func, metric_func);
    test_func<2,    2>(min_func, max_func, metric_func);
    test_func<2,   64>(min_func, max_func, metric_func);
    test_func<99,   1>(min_func, max_func, metric_func);
    test_func<99,   2>(min_func, max_func, metric_func);

    /* These are expected to fail because we exceed the max of 256 slices */
    // test_func<99,  64, false>(min_func, max_func, metric_func);
    // test_func<999, 64, false>(min_func, max_func, metric_func);

    min_func = [] (size_t n_docs, size_t n_metric) { return n_docs;                   };
    max_func = [] (size_t n_docs, size_t n_metric) { return n_docs + (1 << n_metric); };
    test_func<1, 1>(min_func, max_func, metric_func);
    test_func<1, 7>(min_func, max_func, metric_func);
    test_func<1, 15>(min_func, max_func, metric_func);
    test_func<1, 31>(min_func, max_func, metric_func);
    test_func<1, 63>(min_func, max_func, metric_func);
    test_func<99, 1>(min_func, max_func, metric_func);
    /*
      test_func<99, 7>(min_func, max_func, metric_func);
      test_func<99, 15>(min_func, max_func, metric_func);
      test_func<99, 31>(min_func, max_func, metric_func);
      test_func<99, 60>(min_func, max_func, metric_func);
    */
  }
}

