#pragma once

#include <stdint.h>


struct bit_tree {
    int32_t depth;
    int32_t size;
    uint32_t len;
    uint64_t *restrict bitsets;
};


void bit_tree_init(struct bit_tree *tree, int32_t size);
void bit_tree_destroy(struct bit_tree *tree);
void bit_tree_set(struct bit_tree *tree, int32_t idx);
void bit_tree_iterate(struct bit_tree *tree, uint32_t *idx_arr, int32_t len);
int32_t bit_tree_dump(struct bit_tree *tree, uint32_t *restrict idx_arr, int32_t len);
