#include <stdlib.h>
#include <stdint.h>
#include <emmintrin.h>

//struct circular_buffer {
//	uint64_t groups_head;
//	uint64_t groups_tail;
//	uint64_t metrics_head;
//	uint64_t metrics_tail;
//	uint64_t metrics_len;
//	uint64_t metrics_mask;
//	uint64_t groups_len;
//	uint64_t groups_mask;
//	__m128i *restrict metrics_buffer;
//	uint32_t *restrict groups_buffer;
//};

struct circular_buffer {
	uint64_t head;
	uint64_t tail;
	uint64_t len;
	uint64_t mask;
	char *restrict buffer;
};


static struct circular_buffer *alloc(int data_size, int count)
{
	struct circular_buffer *buff;

	buff = (struct circular_buffer *)calloc(sizeof(struct circular_buffer), 1);
	buff->head = 0;
	buff->tail = 0;

	//this would be more clear using __builtin_clz
	//also is num stats the real num_stats or the number of stats plus padding?
	/* bump metrics to the next high power of 2 */
	uint32_t v = count;

	v--;
	v |= v >> 1;
	v |= v >> 2;
	v |= v >> 4;
	v |= v >> 8;
	v |= v >> 16;
	v++;

	buff->len = v;
	buff->mask = v - 1;

	/* align buffers on cache lines */
	buff->buffer = (char *restrict)aligned_alloc(64, data_size * buff->len);

	return buff;
}

static void cleanup(struct circular_buffer *buff)
{
	free(buff->buffer);
	free(buff);
}


/*
 *
 * Some typesafe versions
 *
 */

struct circular_buffer_int;
struct circular_buffer_vector;

struct circular_buffer_int *circular_buffer_int_alloc(int count)
{
	return (struct circular_buffer_int *)alloc(sizeof(int), count);
}

struct circular_buffer_vector *circular_buffer_vector_alloc(int count)
{
	return (struct circular_buffer_vector *)alloc(sizeof(__m128i), count);
}

void circular_buffer_int_cleanup(struct circular_buffer_int *buff)
{
	struct circular_buffer *circ_buf = (struct circular_buffer *)buff;

	cleanup(circ_buf);
}

void circular_buffer_vector_cleanup(struct circular_buffer_vector *buff)
{
	struct circular_buffer *circ_buf = (struct circular_buffer *)buff;

	cleanup(circ_buf);
}

void circular_buffer_int_put(struct circular_buffer_int *buff, uint32_t data)
{
	struct circular_buffer *circ_buf = (struct circular_buffer *)buff;

	circ_buf->buffer[circ_buf->head & circ_buf->mask] = data;
	circ_buf->head ++;
}


void circular_buffer_vector_put(struct circular_buffer_vector *buff, __m128i data)
{
	struct circular_buffer *circ_buf = (struct circular_buffer *)buff;
	__m128i *restrict buffer;

	buffer = (__m128i *)circ_buf->buffer;
	buffer[circ_buf->head & circ_buf->mask] = data;
	circ_buf->head ++;
}

uint32_t circular_buffer_int_get(struct circular_buffer_int *buff)
{
	struct circular_buffer *circ_buf = (struct circular_buffer *)buff;
	uint32_t *restrict buffer;
	uint32_t data;

	buffer = (uint32_t *)circ_buf->buffer;
	data = buffer[circ_buf->tail & circ_buf->mask];
	circ_buf->tail ++;
	return data;
}

__m128i circular_buffer_vector_get(struct circular_buffer_vector *buff)
{
	struct circular_buffer *circ_buf = (struct circular_buffer *)buff;
	__m128i *restrict buffer;
	__m128i data;

	buffer = (__m128i *)circ_buf->buffer;
	data = buffer[circ_buf->tail & circ_buf->mask];
	circ_buf->tail ++;
	return data;
}


