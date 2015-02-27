#include <stdlib.h>
#include <stdio.h>							/* !@# debug */
#include "imhotep_native.h"
#include "circ_buf.h"

#define MAX_BIT_FIELDS					4
#define GROUP_SIZE						28
int const GROUP_MASK = 0xFFFFFFF;


static int metric_size_bytes(struct packed_metric_desc *desc, int64_t max, int64_t min)
{
	uint64_t range = max - min;
	int bits = sizeof(range) * 8 - __builtin_clzl(range); /* !@# fix for zero case */
	if ((bits <= 1) && (desc->n_boolean_metrics == desc->n_metrics_aux_index)
			&& (desc->n_boolean_metrics < MAX_BIT_FIELDS)) {
		desc->n_boolean_metrics++;
		return 0;
	}
	int size = ((bits - 1) / 8) + 1;
	return size;
}

/* Creates the starting and end indexes of the metrics, where metric/16 indicates
 * in which vector the metric is.
 */
static void createMetricsIndexes(	struct packed_metric_desc *desc,
						int n_metrics,
						int64_t * restrict metric_maxes,
						int64_t * restrict metric_mins,
						uint8_t first_free_byte)
{
	int metric_offset = first_free_byte;    //Find the initial byte for the metrics.
	int n_vectors = 1;
	/* Pack the metrics and create indexes to find where they start and end */
	for (int i = 0; i < n_metrics; i++) {
		int metric_size;
        
		metric_size = metric_size_bytes(desc, metric_maxes[i], metric_mins[i]);
		if (metric_offset + metric_size > n_vectors * 16) {
			metric_offset = n_vectors * 16;
			n_vectors++;
		}
		(desc->index_metrics)[2 * i] = metric_offset;
		metric_offset += metric_size;
		(desc->index_metrics)[2 * i + 1] = metric_offset;
		(desc->metric_n_vector)[i] = n_vectors - 1;
		desc->n_metrics_aux_index++;
	}

	/* Calculate how many metrics we have per vector */
	desc->n_metrics_per_vector = (uint8_t *) calloc(sizeof(uint8_t), n_vectors);

	/* initialize and clear the vector */
	for (int i = 0; i < n_vectors; i++) {
		(desc->n_metrics_per_vector)[i] = 0;
	}

	/* Count how many non-bitfield metrics in each packed vector */
	for (int i = desc->n_boolean_metrics; i < n_metrics; i++) {
		(desc->n_metrics_per_vector)[(desc->metric_n_vector)[i]]++;
	}
	desc->n_vectors_per_doc = n_vectors;

	/* calculate the number of vectors in the grp_stats array */
	int grp_stat_row_size = 0;
	for (int i = 0; i < n_vectors; i++) {
		int n_grp_stats_vecs_per_packed_metric = ((desc->n_metrics_per_vector)[i] + 1) / 2;
		grp_stat_row_size += n_grp_stats_vecs_per_packed_metric;
	}
	desc->n_stat_vecs_per_grp = grp_stat_row_size;

	/*
	 * Group stats row size must be 1 or a multiple of 2 vectors
	 * to make preloading work properly
	 */
	if (grp_stat_row_size == 1) {
		desc->grp_stat_size = 1;
	} else {
		/* round up to the next multiple of 2 */
		desc->grp_stat_size = (grp_stat_row_size + 1) & (~0x1);
	}

}

//Create the array that after  can be used to get 2 metrics at a time from the main 
//vector array.**
//**except when there is an odd number of integer metrics in the vector;  
static void createShuffleVecFromIndexes(struct packed_metric_desc *desc)
{
	uint8_t byte_vector[16];
	int k;
	uint8_t n_boolean_metrics = desc->n_boolean_metrics;
	uint8_t n_metrics = desc->n_metrics;
	uint16_t * index_metrics = desc->index_metrics;
	uint8_t * metric_n_vector = desc->metric_n_vector;

	desc->shuffle_vecs_get2 = calloc(sizeof(__v16qi), n_metrics - n_boolean_metrics);
	desc->shuffle_vecs_get1 = calloc(sizeof(__v16qi), n_metrics - n_boolean_metrics);
	int index2 = 0;
	int index1 = 0;

	for (int i = n_boolean_metrics; i < n_metrics; i++) {

		//clears the vector
		for (int j = 0; j < 16; j++) {
			byte_vector[j] = -1;
		}
		//creates the first part of the __m128i vector
		k = 0;
		for (int j = index_metrics[2 * i]; j < index_metrics[2 * i + 1]; j++) {
			byte_vector[k++] = j % 16;
		}
		desc->shuffle_vecs_get1[index1++] = _mm_setr_epi8(byte_vector[0],
												byte_vector[1],
												byte_vector[2],
												byte_vector[3],
												byte_vector[4],
												byte_vector[5],
												byte_vector[6],
												byte_vector[7],
												-1,
												-1,
												-1,
												-1,
												-1,
												-1,
												-1,
												-1);

		//creates the second part of the vector, if the other metric is in the same vector
		//as the first metric. If not, just put -1s in this half of shuffle_vecs_get2
		k = 8;
		i++;
		if (i < n_metrics && (metric_n_vector[i] == metric_n_vector[i - 1])) {
			for (int j = index_metrics[2 * i]; j < index_metrics[2 * i + 1]; j++) {
				byte_vector[k++] = j % 16;
			}
			desc->shuffle_vecs_get1[index1++] = _mm_setr_epi8(byte_vector[8],
													byte_vector[9],
													byte_vector[10],
													byte_vector[11],
													byte_vector[12],
													byte_vector[13],
													byte_vector[14],
													byte_vector[15],
													-1,
													-1,
													-1,
													-1,
													-1,
													-1,
													-1,
													-1);
		} else {
			i--;
		}
		desc->shuffle_vecs_get2[index2++] = _mm_setr_epi8(byte_vector[0],
												byte_vector[1],
												byte_vector[2],
												byte_vector[3],
												byte_vector[4],
												byte_vector[5],
												byte_vector[6],
												byte_vector[7],
												byte_vector[8],
												byte_vector[9],
												byte_vector[10],
												byte_vector[11],
												byte_vector[12],
												byte_vector[13],
												byte_vector[14],
												byte_vector[15]);
	}
}

//Creates the shuffle and Blends vectors used to put metrics inside the vector.
static void createShuffleBlendFromIndexes(struct packed_metric_desc * desc)
{
	uint8_t byte_vector_shuffle[16];
	uint8_t byte_vector_blend[16];
	uint8_t n_boolean_metrics = desc->n_boolean_metrics;
	uint8_t n_metrics = desc->n_metrics;
	uint16_t * index_metrics = desc->index_metrics;
	int k, i, j;
	desc->shuffle_vecs_put = calloc(sizeof(__v16qi ), (n_metrics - n_boolean_metrics));
	desc->blend_vecs_put = calloc(sizeof(__v16qi ), (n_metrics - n_boolean_metrics));

	//Creates the shuffle vectors to put each metric in the right place for blending
	// And create the blend vectors. We will have a main vector that is gonna receive
	// all the metrics one at a time by blending.
	// with the exception of the boolean metrics.
	for (i = n_boolean_metrics; i < n_metrics; i++) {
		int index = i - n_boolean_metrics;
		for (j = 0; j < 16; j++) {
			byte_vector_shuffle[j] = -1;
			byte_vector_blend[j] = 0;
		}
		k = 0;
		for (j = index_metrics[2 * i]; j < index_metrics[2 * i + 1]; j++) {
			byte_vector_shuffle[j % 16] = k++;
			byte_vector_blend[j % 16] = -1;
		}
		desc->shuffle_vecs_put[index] = _mm_setr_epi8(	byte_vector_shuffle[0],
												byte_vector_shuffle[1],
												byte_vector_shuffle[2],
												byte_vector_shuffle[3],
												byte_vector_shuffle[4],
												byte_vector_shuffle[5],
												byte_vector_shuffle[6],
												byte_vector_shuffle[7],
												byte_vector_shuffle[8],
												byte_vector_shuffle[9],
												byte_vector_shuffle[10],
												byte_vector_shuffle[11],
												byte_vector_shuffle[12],
												byte_vector_shuffle[13],
												byte_vector_shuffle[14],
												byte_vector_shuffle[15]);

		desc->blend_vecs_put[index] = _mm_setr_epi8(	byte_vector_blend[0],
											byte_vector_blend[1],
											byte_vector_blend[2],
											byte_vector_blend[3],
											byte_vector_blend[4],
											byte_vector_blend[5],
											byte_vector_blend[6],
											byte_vector_blend[7],
											byte_vector_blend[8],
											byte_vector_blend[9],
											byte_vector_blend[10],
											byte_vector_blend[11],
											byte_vector_blend[12],
											byte_vector_blend[13],
											byte_vector_blend[14],
											byte_vector_blend[15]);
	}
}

//This method assumes that the boolean metrics will come first
void packed_shard_init(	packed_shard_t *shard,
					uint32_t n_docs,
					int64_t *metric_mins,
					int64_t *metric_maxes,
					int n_metrics)
{
	struct packed_metric_desc *desc;

	shard->num_docs = n_docs;

	desc = (struct packed_metric_desc *)calloc(sizeof(struct packed_metric_desc), 1);
	shard->metrics_layout = desc;

	desc->index_metrics = (uint16_t *) calloc(sizeof(uint16_t), n_metrics * 2);
	desc->metric_n_vector = (uint8_t *) calloc(sizeof(uint8_t), n_metrics);
	desc->n_metrics = n_metrics;
	/* ensure that this is >= to the smallest multiple if 16 the data can fit in */
	desc->metric_mins = (int64_t *) aligned_alloc(16, sizeof(int64_t) * n_metrics + 1);
	desc->n_metrics_aux_index = 0;
	for (int i = 0; i < n_metrics; i++) {
		desc->metric_mins[i] = metric_mins[i];
	}
	desc->n_boolean_metrics = 0;
	createMetricsIndexes(	desc,
						n_metrics,
						metric_maxes,
						metric_mins,
						(GROUP_SIZE + MAX_BIT_FIELDS + 7) / 8);
	createShuffleVecFromIndexes(desc);
	createShuffleBlendFromIndexes(desc);

	shard->grp_metrics_len = n_docs * desc->n_vectors_per_doc;
	shard->groups_and_metrics = (__v16qi *) aligned_alloc(	64,
	                                                      	sizeof(__m128i ) * shard->grp_metrics_len);
}


void packed_shard_destroy(packed_shard_t *shard)
{
	struct packed_metric_desc *desc;

	shard->num_docs = -1;
	shard->shard_id = 0;

	desc = shard->metrics_layout;
	free(desc->index_metrics);
	free(desc->metric_n_vector);
	free(desc->metric_mins);
	free(desc->shuffle_vecs_get1);
	free(desc->shuffle_vecs_put);
	free(desc->blend_vecs_put);
	free(desc->shuffle_vecs_get2);
	free(desc->n_metrics_per_vector);
	free(desc);

	free(shard->groups_and_metrics);
}

static void update_boolean_metric(	packed_shard_t *shard,
								int * restrict doc_ids,
								int n_doc_ids,
								int64_t * restrict metric_vals,
								int metric_index)
{
	struct packed_metric_desc *desc = shard->metrics_layout;
	int64_t min = (desc->metric_mins)[metric_index];

	for (int i = 0; i < n_doc_ids; i++) {
		__v16qi *packed_addr;
		uint32_t *store_address;
		int index = doc_ids[i] * desc->n_vectors_per_doc + 0;

		packed_addr = &shard->groups_and_metrics[index];
		store_address = (uint32_t *)packed_addr;
		*store_address |= (metric_vals[i] - min) << (GROUP_SIZE + metric_index);
	}
}

void packed_shard_update_metric(	packed_shard_t *shard,
							int * restrict doc_ids,
							int n_doc_ids,
							int64_t * restrict metric_vals,
							int metric_index)
{
	struct packed_metric_desc *desc = shard->metrics_layout;
	int64_t min = (desc->metric_mins)[metric_index];
	uint8_t packed_vector_index = (desc->metric_n_vector)[metric_index];

	if (metric_index < desc->n_boolean_metrics) {
		update_boolean_metric(shard, doc_ids, n_doc_ids, metric_vals, metric_index);
		return;
	}
	
	metric_index -= desc->n_boolean_metrics;
	for (int i = 0; i < n_doc_ids; i++) {
		size_t index = doc_ids[i] * desc->n_vectors_per_doc;

		/* this makes the assumption that the first doc id is doc_id[0] */
		size_t vector_index = packed_vector_index + index;

		/* Converts the data to a vector and shuffles the bytes into the correct spot */
		__m128i shuffled_metric = _mm_shuffle_epi8(	_mm_cvtsi64_si128(metric_vals[i] - min),
											desc->shuffle_vecs_put[metric_index]);

		/* Inserts the new data into the packed data vector */
		__v16qi packed_data = shard->groups_and_metrics[vector_index];
		__v16qi updated_data = _mm_blendv_epi8(packed_data,
										shuffled_metric,
										desc->blend_vecs_put[metric_index]);
		shard->groups_and_metrics[vector_index] = updated_data;
	}
}

void packed_shard_lookup_metric_values(	packed_shard_t *shard,
								int * restrict doc_ids,
								int n_doc_ids,
								int64_t * restrict dest,
								int metric_index)
{
	struct packed_metric_desc *desc = shard->metrics_layout;
	uint8_t n_boolean_metrics = desc->n_boolean_metrics;
	int n_vecs_per_doc = desc->n_vectors_per_doc;
	int64_t min = (desc->metric_mins)[metric_index];

	if (metric_index >= n_boolean_metrics) {
		uint8_t metric_vector = (desc->metric_n_vector)[metric_index];
		for (int i = 0; i < n_doc_ids; i++) {
			int doc_number;
			__v16qi packed;
			__v16qi shuffle_control_vec;
			__m128i unpacked;
			int64_t result;

			doc_number = doc_ids[i];
			packed = (shard->groups_and_metrics)[doc_number * n_vecs_per_doc + metric_vector];
			shuffle_control_vec = (desc->shuffle_vecs_get1)[metric_index - n_boolean_metrics];
			unpacked = _mm_shuffle_epi8(packed, shuffle_control_vec);
			result = _mm_extract_epi64(unpacked, 0);
			dest[i] = result + min;
		}
	} else {
		for (int i = 0; i < n_doc_ids; i++) {
			__v16qi *packed_addr;
			uint32_t *load_address;
			uint32_t bit;
			int index = doc_ids[i] * n_vecs_per_doc + 0;

			packed_addr = &shard->groups_and_metrics[index];
			load_address = (uint32_t *)packed_addr;
			bit = (*load_address) & (1 << (GROUP_SIZE + metric_index));
			dest[i] = (bit != 0) + min;
		}
	}
}

void packed_shard_lookup_groups(packed_shard_t *shard,
                                int * restrict doc_ids,
                                int n_doc_ids,
                                int64_t * restrict groups)
{
	const struct packed_metric_desc *desc = shard->metrics_layout;

	for (int i = 0; i < n_doc_ids; i++) {
		__v16qi *packed_addr;
		const int index = doc_ids[i] * desc->n_vectors_per_doc + 0;
		packed_addr = &shard->groups_and_metrics[index];
		const struct bit_fields_and_group *packed_bf_grp = (struct bit_fields_and_group *)packed_addr;
		groups[i] = packed_bf_grp->grp;
	}
}

void packed_shard_update_groups(packed_shard_t *shard,
                                int * restrict doc_ids,
                                int n_doc_ids,
                                int64_t * restrict groups)
{
	const struct packed_metric_desc *desc = shard->metrics_layout;

	for (int i = 0; i < n_doc_ids; i++) {
		__v16qi *packed_addr;
		const int index = doc_ids[i] * desc->n_vectors_per_doc + 0;
		packed_addr = &shard->groups_and_metrics[index];
		struct bit_fields_and_group *packed_bf_grp = (struct bit_fields_and_group *)packed_addr;
		packed_bf_grp->grp = groups[i] & GROUP_MASK;
	}
}
``

static int unpack_bit_fields(struct circular_buffer_vector *buffer,
						uint32_t bit_fields,
						uint8_t n_bit_fields)
{
	static __m128i lookup_table[4] = { { 0L, 0L }, { 0L, 1L }, { 1L, 0L }, { 1L, 1L } };
	int i;

	for (i = 0; i < n_bit_fields; i += 2) {
		circular_buffer_vector_put(buffer, lookup_table[bit_fields & 3]);
		bit_fields >>= 2;
	}
	return i / 2;
}

static inline __m128i unpack_2_metrics(__m128i packed_data, __v16qi shuffle_vector)
{
	__m128i unpacked;

	unpacked = _mm_shuffle_epi8(packed_data, shuffle_vector);
	return unpacked;
}

static void load_less_than_a_cache_line_of_metrics(struct packed_metric_desc *desc,
										 __v16qi *restrict grp_metrics,
										 int doc_id, uint8_t *restrict n_metrics_per_vector,
										 struct circular_buffer_vector *buffer, int prefetch_doc_id)
{
	uint32_t shuffle_idx = 0;
	int n_vecs_per_doc = desc->n_vectors_per_doc;
	
	__v2di *mins;
	mins = (__v2di *)desc->metric_mins;
	
	__v16qi* doc_grp_metrics = grp_metrics+doc_id*n_vecs_per_doc;

	for (int32_t vector_idx = 0; vector_idx < desc->n_vectors_per_doc; vector_idx++) {
		__m128i packed_data = doc_grp_metrics[vector_idx];
		for (int32_t k = 0; k < n_metrics_per_vector[vector_idx]; k += 2, shuffle_idx++) {
			__m128i data;
			__m128i decoded_data;

			data = unpack_2_metrics(packed_data, desc->shuffle_vecs_get2[shuffle_idx]);
			decoded_data = _mm_add_epi64(data, mins[shuffle_idx]);


			/* save data into buffer */
			circular_buffer_vector_put(buffer, decoded_data);
		}
	}
	if (prefetch_doc_id != 0) {
        _mm_prefetch(grp_metrics+prefetch_doc_id*n_vecs_per_doc, _MM_HINT_T0);
    }
}

static void load_cache_line_full_of_metrics(struct packed_metric_desc *desc,
								__v16qi *restrict grp_metrics,
								int doc_id,
								uint8_t* restrict n_metrics_per_vector,
                                   	struct circular_buffer_vector *buffer,
                                   	int prefetch_doc_id)
{
    uint32_t vector_index = 0;
	int n_vecs_per_doc = desc->n_vectors_per_doc;
	
	__v2di *mins;
	mins = (__v2di *)desc->metric_mins;
	
	__v16qi* doc_grp_metrics = grp_metrics+doc_id*n_vecs_per_doc;
	__v16qi* shuffle_vecs = desc->shuffle_vecs_get2;
	
     //unrolled loop for processing whole 64 byte cache lines at a time (4x16 byte loads)
     for (int32_t load_index = 0; load_index <= n_vecs_per_doc-4; load_index+=4) {
		//load the next 16 bytes
		__m128i packed_data = doc_grp_metrics[load_index];
		
		//for each vector, shuffle to get next 2 metrics, load current sum, add metrics to current sum, store result. zig zag decode if necessary.
		for (int32_t k = 0; k < n_metrics_per_vector[load_index]; k += 2) {
			__m128i data;
			__m128i decoded_data;
			
			data = unpack_2_metrics(packed_data, shuffle_vecs[vector_index]);
			decoded_data = _mm_add_epi64(data, mins[vector_index]);

			/* save data into buffer */
			circular_buffer_vector_put(buffer, decoded_data);
		
			vector_index++;
		}

          packed_data = doc_grp_metrics[load_index + 1];
          for (int32_t k = 0; k < n_metrics_per_vector[load_index + 1]; k += 2) {
              __m128i data;
              __m128i decoded_data;

              data = unpack_2_metrics(packed_data, shuffle_vecs[vector_index]);

		      decoded_data = _mm_add_epi64(data, mins[vector_index]);

              /* save data into buffer */
              circular_buffer_vector_put(buffer, decoded_data);

              vector_index++;
          }

          packed_data = doc_grp_metrics[load_index + 2];
          for (int32_t k = 0; k < n_metrics_per_vector[load_index + 2]; k += 2) {
              __m128i data;
              __m128i decoded_data;

              data = unpack_2_metrics(packed_data, shuffle_vecs[vector_index]);
		      decoded_data = _mm_add_epi64(data, mins[vector_index]);

              /* save data into buffer */
              circular_buffer_vector_put(buffer, decoded_data);

              vector_index++;
          }

          packed_data = doc_grp_metrics[load_index + 3];
          for (int32_t k = 0; k < n_metrics_per_vector[load_index + 3]; k += 2) {
              __m128i data;
              __m128i decoded_data;

              data = unpack_2_metrics(packed_data, shuffle_vecs[vector_index]);
		      decoded_data = _mm_add_epi64(data, mins[vector_index]);

              /* save data into buffer */
              circular_buffer_vector_put(buffer, decoded_data);

              vector_index++;
          }
          if (prefetch_doc_id != 0) {
              _mm_prefetch(grp_metrics+prefetch_doc_id*n_vecs_per_doc+load_index, _MM_HINT_T0);
          }
        
     }

}

void packed_shard_unpack_metrics_into_buffer(packed_shard_t *shard,
									int doc_id,
									struct circular_buffer_vector *buffer,
									int prefetch_doc_id)
{
	struct packed_metric_desc *desc = shard->metrics_layout;
	uint32_t bit_fields;
	int index = doc_id * desc->n_vectors_per_doc;

	/* unpack and save the bit fields */
	bit_fields = _mm_extract_epi32(shard->groups_and_metrics[index], 0);
	bit_fields = bit_fields >> GROUP_SIZE;
	unpack_bit_fields(buffer, bit_fields, desc->n_boolean_metrics);

	/* unpack and save the metrics */
	if ((desc->n_vectors_per_doc + 3) / 4 <= 1) {
		
		load_less_than_a_cache_line_of_metrics(	desc,
										shard->groups_and_metrics,
										doc_id,
										desc->n_metrics_per_vector,
										buffer, prefetch_doc_id);
     } else {
          load_cache_line_full_of_metrics(desc, shard->groups_and_metrics, doc_id, desc->n_metrics_per_vector,
                                          buffer, prefetch_doc_id);
	}
}

void dump_shard(packed_shard_t *shard)
{
	static char digits[16] = { '0', '1', '2', '3', '4', '5', '6', '7',
														 '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	const size_t size = sizeof(__m128i ) * shard->grp_metrics_len;
	fprintf(stderr, "desc->n_vectors_per_doc: %d\n", shard->metrics_layout->n_vectors_per_doc);
	for (size_t i = 0; i < size; ++i) {
		uint8_t value = ((const uint8_t *) shard->groups_and_metrics)[i];
		char hex[3] = { digits[value >> 4], digits[value & 0x0f], '\0' };
		fprintf(stderr, "%s ", hex);
		if (i % 16 == 15) fprintf(stderr, "\n");
	}
}
