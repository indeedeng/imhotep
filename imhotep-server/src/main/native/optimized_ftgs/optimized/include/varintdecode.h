#ifndef _VARINTDECODE_H_
#define _VARINTDECODE_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

void init(void);

size_t masked_vbyte_read_loop_delta(const uint8_t* in, uint32_t* out,
		uint64_t length, uint32_t prev);

#ifdef __cplusplus
}
#endif

#endif
