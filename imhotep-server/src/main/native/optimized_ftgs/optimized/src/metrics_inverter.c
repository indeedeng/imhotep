#include <stdint.h>
#include "imhotep_native.h"
#include "varintdecode.h"

#define BUFFER_SIZE								8192


#define DATA_TYPE								int64_t
#define TYPE_NAME								long

#include "metrics_inverter.template"

#undef DATA_TYPE
#undef TYPE_NAME
#define DATA_TYPE								int32_t
#define TYPE_NAME								int

#include "metrics_inverter.template"

#undef DATA_TYPE
#undef TYPE_NAME
#define DATA_TYPE								int16_t
#define TYPE_NAME								short

#include "metrics_inverter.template"

#undef DATA_TYPE
#undef TYPE_NAME
#define DATA_TYPE								int8_t
#define TYPE_NAME								byte

#include "metrics_inverter.template"


/*
 *  Bit fields code:
 */
static inline uint64_t set_bit(int64_t value, uint32_t offset)
{
	return value | (1 << offset);
}

static inline void invert_bitfield_terms(int64_t * restrict data_buf,
                                         const uint32_t* restrict doc_id_buf,
                                         const int count)
{
	const uint32_t mask = 64 - 1;

	int i = 0;
	for (; i < count - PREFETCH_DISTANCE; i++) {
		const uint32_t doc_id = doc_id_buf[i + 0];
		const uint32_t prefetch_doc_id = doc_id_buf[i + 0 + PREFETCH_DISTANCE];
		const uint32_t offset = doc_id >> 6;
		const uint32_t prefetch_offset = prefetch_doc_id >> 6;

		data_buf[offset] = set_bit(data_buf[offset], doc_id & mask);

		__builtin_prefetch(&data_buf[prefetch_offset], 1, 3);
	}
	for (; i < count; i++) {
		const uint32_t doc_id = doc_id_buf[i];
		const uint32_t offset = doc_id >> 6;

		data_buf[offset] = set_bit(data_buf[offset], doc_id & mask);
	}
}

int invert_bitfield_metric(int64_t * restrict data_buf,
                           const int32_t n_docs,
                           const uint8_t* restrict doc_list_address,
                           const int64_t offset)
{
	uint32_t doc_id_buf[BUFFER_SIZE];

	int remaining = n_docs;     /* num docs remaining */
	const uint8_t* restrict read_addr = doc_list_address + offset;
	int last_value;     /* delta decode tracker */

	last_value = 0;
	while (remaining > 0) {
		int count;
		int bytes_read;

		count = (remaining > BUFFER_SIZE) ? BUFFER_SIZE : remaining;
		bytes_read = masked_vbyte_read_loop_delta(read_addr, doc_id_buf, count, last_value);
		read_addr += bytes_read;
		remaining -= count;

		invert_bitfield_terms(data_buf, doc_id_buf, count);
		last_value = doc_id_buf[count - 1];
	}

	return 0;
}
