#ifndef _VARINTDECODE_H_
#define _VARINTDECODE_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

void init(void);

size_t read_ints(uint32_t, uint8_t*, uint32_t*, int);

size_t read_ints_single(uint32_t, uint8_t*, uint32_t*, int);

#ifdef __cplusplus
}
#endif

#endif
