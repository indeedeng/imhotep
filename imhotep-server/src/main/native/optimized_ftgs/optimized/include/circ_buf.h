#include <stdint.h>
#include <emmintrin.h>

struct circular_buffer_int;
struct circular_buffer_vector;

struct circular_buffer_int *circular_buffer_int_alloc(int32_t size);
struct circular_buffer_vector *circular_buffer_vector_alloc(int32_t size);
void circular_buffer_int_cleanup(struct circular_buffer_int *buff);
void circular_buffer_vector_cleanup(struct circular_buffer_vector *buff);
void circular_buffer_int_put(struct circular_buffer_int *buff, uint32_t data);
void circular_buffer_vector_put(struct circular_buffer_vector *buff, __m128i data);
uint32_t circular_buffer_int_get(struct circular_buffer_int *buff);
__m128i circular_buffer_vector_get(struct circular_buffer_vector *buff);
