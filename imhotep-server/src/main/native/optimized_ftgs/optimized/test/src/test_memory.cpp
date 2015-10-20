extern "C" {
#include "memory.h"
}

#include "imhotep_error.hpp"

#include <iostream>
#include <stdexcept>
#include <stdio.h>

int main(int argc, char* argv[]) {
    memory_ptr pool = memory_create("/tmp/fubar", "floopy", 4096 * 4096);
    fprintf(stdout, "pool: %p allocated: %lu remaining: %lu filename: %s\n",
            pool,
            memory_allocated(pool),
            memory_remaining(pool),
            memory_filename(pool));
    try {
        size_t remaining(memory_remaining(pool));
        while (remaining > 0) {
            void* item(memory_calloc(pool, remaining / 3, 1));
            fprintf(stdout, "item: %p allocated: %lu remaining: %lu\n",
                    item,
                    memory_allocated(pool),
                    memory_remaining(pool));
            remaining = memory_remaining(pool);
        }
        void* item(memory_malloc(pool, 1));
        memory_destroy(pool);
    }
    catch (imhotep::imhotep_error ex) {
        std::cerr << ex.what() << std::endl;
        memory_destroy(pool);
    }
}
