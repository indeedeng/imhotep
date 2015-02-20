#include "imhotep_native.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

void test_packed_shards(uint32_t n_docs,
												int64_t* mins,
												int64_t* maxes,
												int      n_metrics)
{
	fprintf(stdout, "(min, max): ");
	for (int metric_index = 0; metric_index < n_metrics; ++metric_index) {
		fprintf(stdout, "(%ld, %ld) ", mins[metric_index], maxes[metric_index]);
	}
	fprintf(stdout, "\n");

	packed_shard_t shard;
	packed_shard_init(&shard, n_docs, mins, maxes, n_metrics);
		
	/* fprintf(stderr, "after init:\n"); */
	/* dump_shard(&shard); */

  for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
		int64_t group = doc_id;
		packed_shard_update_groups(&shard, &doc_id, 1, &group);
  }

	/* fprintf(stderr, "after groups:\n"); */
	/* dump_shard(&shard); */

	for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
		int64_t expected = doc_id;
		int64_t actual[1] = { -1 };
		packed_shard_lookup_groups(&shard, &doc_id, 1, actual);
		if (expected != actual[0]) {
			fprintf(stderr, "group lookup failed expected: %ld actual: %ld\n", expected, actual[0]);
		}
	}

	for (int metric_index = 0; metric_index < n_metrics; ++metric_index) {
		int64_t* metrics = calloc(n_metrics, sizeof(int64_t));
		for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
			metrics[metric_index] = mins[metric_index] + doc_id;
			packed_shard_update_metric(&shard, &doc_id, 1, metrics, metric_index);
			/* fprintf(stderr, "doc_id: %d metric_index: %d\n", doc_id, metric_index); */
			/* dump_shard(&shard); */
		}
		free(metrics);
	}

	for (int metric_index = 0; metric_index < n_metrics; ++metric_index) {
		int64_t* metrics = calloc(n_metrics, sizeof(int64_t));
		for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
			int64_t expected = mins[metric_index] + doc_id;
			packed_shard_lookup_metric_values(&shard, &doc_id, 1, metrics, metric_index);
			if (metrics[metric_index] != expected) {
					fprintf(stderr,
									"error: metric mismatch doc_id: %d i_metric: %d"
									" expected: %ld actual %ld\n",
									doc_id, metric_index, expected, metrics[metric_index]);
				}
		}
		free(metrics);
	}
	packed_shard_destroy(&shard);
}

void test_booleans(int n_docs)
{
	{
		int n_metrics = 4;
		int64_t mins[4] = { 0, 0, 0, 0 };
		int64_t maxes[4] = { 1, 1, 1, 1 };
		test_packed_shards(n_docs, mins, maxes, n_metrics);
	}
	{
		int n_metrics = 5;
		int64_t mins[5] = { 0, 0, 0, 0, 0 };
		int64_t maxes[5] = { 1, 1, 1, 1, 1 };
		test_packed_shards(n_docs, mins, maxes, n_metrics);
	}
}

void test_bytes(int n_docs)
{
	{
		int n_metrics = 1;
		int64_t mins[1] = { 0 };
		int64_t maxes[1] = { 0xf };
		test_packed_shards(n_docs, mins, maxes, n_metrics);
	}
	{
		int n_metrics = 8;
		int64_t mins[8] = { 0, 0, 0, 0, 0, 0, 0, 0 };
		int64_t maxes[8] = { 0x0f, 0x0f, 0x0f, 0x0f, 0x0f, 0x0f, 0x0f, 0x0f };
		test_packed_shards(n_docs, mins, maxes, n_metrics);
	}
}

int main(int argc, char * argv[])
{
	if (argc != 2) {
		fprintf(stderr, "usage %s <n_docs>\n", argv[0]);
		exit(1);
	}

	int n_docs = atoi(argv[1]);
	test_booleans(n_docs);
	test_bytes(n_docs);
}

