#include <stdlib.h>
#include "imhotep_native.h"
#include "varintdecode.h"

#include <stdint.h>

#define likely(x)   __builtin_expect((x),1)
#define unlikely(x) __builtin_expect((x),0)

#define TGS_BUFFER_SIZE 2048

#define TYPE int8_t
#include "groups.template"
#undef TYPE

#define TYPE uint16_t
#include "groups.template"
#undef TYPE

#define TYPE int32_t
#include "groups.template"
#undef TYPE

