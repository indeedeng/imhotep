#define restrict __restrict__
extern "C" {
#include "imhotep_native.h"
}

#include <algorithm>
#include <array>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <vector>

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


typedef function<int64_t(int64_t min, int64_t max)> MetricFunc;

template <size_t n_metrics>
bool test_packed_shards(size_t n_docs,
                        array<int64_t, n_metrics>& mins,
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
			metrics[metric_index] = metric_func(mins[metric_index], maxes[metric_index]);
			packed_shard_update_metric(&shard, &doc_id, 1, &metrics[metric_index], metric_index);
		}
	}

	for (int metric_index = 0; metric_index < n_metrics; ++metric_index) {
    array<int64_t, n_metrics> metrics;
		for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
			int64_t expected = metric_func(mins[metric_index], maxes[metric_index]);
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


typedef function<int64_t(size_t n_metric, size_t metric_index)> RangeFunc;

template <size_t n_metrics,
          bool should_succeed=true>
void test_func(size_t n_docs,
               RangeFunc min_func,
               RangeFunc max_func,
               MetricFunc metric_func)
{
  array<int64_t, n_metrics> mins;
  array<int64_t, n_metrics> maxes;
  for (size_t i(0); i < n_metrics; ++i) {
    mins[i]  = min_func(n_metrics, i);
    maxes[i] = max_func(n_metrics, i);
  }
  const bool result(test_packed_shards<n_metrics>(n_docs, mins, maxes, metric_func));
   // cout << " mins: " << mins << endl;
   // cout << "maxes: " << maxes << endl;
  cout << ((result == should_succeed) ? "PASSED" : "FAILED")
       << " n_docs: "    << setw(10) << left << n_docs
       << " n_metrics: " << setw(10) << left << n_metrics << " "
       << endl;
}


template <size_t n_metrics, int64_t min_value, int64_t max_value,
          bool should_succeed=true>
void test_uniform(size_t n_docs, MetricFunc metric_func)
{
  RangeFunc min_func([](size_t, size_t) { return min_value; });
  RangeFunc max_func([](size_t, size_t) { return max_value; });
  test_func<n_metrics, should_succeed>(n_docs, min_func, max_func, metric_func);
}


/* Make a max func for a given type that we can use to pack four
   booleans into the flags area and elements of a given size into the
   rest. */
template <typename T>
RangeFunc make_flags_test_max_func() {
  return [](size_t, size_t metric_index) {
    return metric_index < 4 ? 1 : numeric_limits<T>::max();
  };
}


int main(int argc, char * argv[])
{
  const size_t max_n_docs(argc == 2 ? atoi(argv[1]) : 1);
  for (size_t n_docs(1); n_docs <= max_n_docs; ++n_docs) {

    vector<MetricFunc> metric_funcs = {
        [](int64_t min_val, int64_t max_val) { return min_val; },
        [](int64_t min_val, int64_t max_val) { return max_val; },
        [](int64_t min_val, int64_t max_val) { return min_val + (max_val - min_val) / 2; },
      };

    for (auto metric_func: metric_funcs) {
      /* We should be able to store 4 booleans in flags and another
         251 in single-byte entries. */
      test_uniform<1,   0, 1>(n_docs, metric_func);
      test_uniform<4,   0, 1>(n_docs, metric_func);
      test_uniform<5,   0, 1>(n_docs, metric_func);
      test_uniform<255, 0, 1>(n_docs, metric_func);

      /* Single entries for each metric size. */
      test_uniform<1, 0, numeric_limits<int8_t>::max()>(n_docs, metric_func);
      test_uniform<1, 0, numeric_limits<int16_t>::max()>(n_docs, metric_func);
      test_uniform<1, 0, numeric_limits<int32_t>::max()>(n_docs, metric_func);
      test_uniform<1, 0, numeric_limits<int64_t>::max()>(n_docs, metric_func);

      /* Full pack of each metric size. */
      test_uniform<251, 0, numeric_limits<int8_t>::max()>(n_docs, metric_func);
      test_uniform<126, 0, numeric_limits<int16_t>::max()>(n_docs, metric_func);
      test_uniform<63,  0, numeric_limits<int32_t>::max()>(n_docs, metric_func);
      test_uniform<31,  0, numeric_limits<int64_t>::max()>(n_docs, metric_func);

      /* Four booleans + full pack of each metric size. */
      RangeFunc min_func([](size_t, size_t) { return 0; });
      test_func<251>(n_docs, min_func, make_flags_test_max_func<int8_t>(), metric_func);
      test_func<126>(n_docs, min_func, make_flags_test_max_func<int16_t>(), metric_func);
      test_func<63>(n_docs, min_func, make_flags_test_max_func<int32_t>(), metric_func);
      test_func<31>(n_docs, min_func, make_flags_test_max_func<int64_t>(), metric_func);
    }
  }
}

