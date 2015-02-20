#include "imhotep_native.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define MAX_METRIC(i_metric) i_metric << (i_metric % 64)

void die_if(int condition, char * message) {
	if (condition) {
		fprintf(stderr, "error: %s\n", message);
		exit(1);
	}
}

void rand_metrics(unsigned int *seed, size_t n_metrics, int64_t *out_metrics)
{
  for (size_t i = 0; i < n_metrics; ++i) {
		int64_t value = rand_r(seed);
		value = (value << 32) | rand_r(seed);
		out_metrics[i] = value >> (i % 56); /* for a variety of ranges */
  }
}

void rand_min_maxes(unsigned int *seed, size_t iterations, size_t n_metrics,
										int64_t *out_mins, int64_t *out_maxes)
{
	fprintf(stderr, "%s\n", __PRETTY_FUNCTION__);
	int64_t * metrics = malloc(sizeof(int64_t) * n_metrics);
	for (size_t it = 0; it < iterations; ++it) {
		rand_metrics(seed, n_metrics, metrics);
		for (size_t mit = 0; mit < n_metrics; ++mit) {
			out_mins[mit] = (out_mins[mit] == -1 || metrics[mit] < out_mins[mit])  ? metrics[mit] : out_mins[mit];
			out_maxes[mit] = (out_mins[mit] == -1 || metrics[mit] > out_maxes[mit]) ? metrics[mit] : out_maxes[mit];
		}
	}
	free(metrics);
} 

#define RESET_SEED			42
#define N_METRICS				16

int main(int argc, char * argv[])
{
	if (argc != 2) {
		fprintf(stderr, "usage %s <n_docs>\n", argv[0]);
		exit(1);
	}

	int n_docs = atoi(argv[1]);

	int64_t mins[N_METRICS];
	int64_t maxes[N_METRICS];

	memset(mins, -1, sizeof(mins));
	memset(maxes, -1, sizeof(maxes));

	unsigned int seed = RESET_SEED;
	rand_min_maxes(&seed, n_docs, N_METRICS, mins, maxes);

	for (int i_metric = 0; i_metric < N_METRICS; ++ i_metric) {
		fprintf(stdout, "i_metric: %d\nmin: %ld\nmax: %ld\ndiff: %ld\n\n",
						i_metric, mins[i_metric], maxes[i_metric],
						maxes[i_metric] - mins[i_metric]);
	}

	packed_shard_t shard;
	packed_shard_init(&shard, n_docs, mins, maxes, N_METRICS);
		
	fprintf(stderr, "before groups:\n");
	dump_shard(&shard);
	for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
		int64_t group = doc_id % N_METRICS;
		packed_shard_update_groups(&shard, &doc_id, 1, &group);
		fprintf(stderr, "doc_id: %d\n", doc_id);
		dump_shard(&shard);
	}

	fprintf(stderr, "before metrics:\n");
	dump_shard(&shard);
	for (int metric_index = 0; metric_index < N_METRICS; ++metric_index) {
		seed = RESET_SEED;
		int64_t metrics[N_METRICS];
		for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
			int doc_ids[1] = { doc_id };
			rand_metrics(&seed, N_METRICS, metrics);
			packed_shard_update_metric(&shard, doc_ids, 1, metrics, metric_index);
			fprintf(stderr, "doc_id: %d metric_index: %d\n", doc_id, metric_index);
			dump_shard(&shard);
		}
	}

	fprintf(stderr, "after groups:\n");
	dump_shard(&shard);
	for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
		int64_t expected = doc_id % N_METRICS;
		int     doc_ids[1] = { doc_id };
		int64_t actual[1]	 = { -1 };
		packed_shard_lookup_groups(&shard, doc_ids, 1, actual);
		fprintf(stderr, "group lookup expected: %ld actual: %ld\n", expected, actual[0]);
		if (expected != actual[0]) {
			fprintf(stderr, "group lookup failed expected: %ld actual: %ld\n", expected, actual[0]);
		}
	}

	fprintf(stderr, "after metrics:\n");
	dump_shard(&shard);
	for (int metric_index = 0; metric_index < N_METRICS; ++metric_index) {
		seed = RESET_SEED;
		int64_t expected[N_METRICS];
		int64_t actual[N_METRICS];
		for (int doc_id = 0; doc_id < n_docs; ++doc_id) {
			int doc_ids[1] = { doc_id };
			rand_metrics(&seed, N_METRICS, expected);
			packed_shard_lookup_metric_values(&shard, doc_ids, 1, actual, metric_index);
			for (int i_metric = 0; i_metric < N_METRICS; ++i_metric) {
				if (expected[i_metric] != actual[i_metric]) {
					fprintf(stderr,
									"error: metric mismatch doc_id: %d i_metric: %d"
									" expected: %ld actual %ld\n",
									doc_id, i_metric, expected[i_metric], actual[i_metric]);
				}
			}
		}
	}

	packed_shard_destroy(&shard);
}

