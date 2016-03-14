#ifndef MEMORY_H
#define MEMORY_H

#include <stdlib.h>

typedef void* memory_ptr;

memory_ptr memory_create(const char* dir, const char* prefix, size_t max_size);
void memory_destroy(memory_ptr memory);

void* memory_malloc(memory_ptr memory, size_t size);
void* memory_calloc(memory_ptr memory, size_t nmemb, size_t size);

const char* memory_filename(memory_ptr);
size_t memory_allocated(memory_ptr memory);
size_t memory_remaining(memory_ptr memory);

#endif
