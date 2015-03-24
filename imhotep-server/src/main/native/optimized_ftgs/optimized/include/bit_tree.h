#pragma once

#include <stdint.h>

struct bit_tree {
    int32_t depth;
    int64_t **restrict bitsets;
};

void bit_tree_init(struct bit_tree *tree, int32_t size);
void bit_tree_destroy(struct bit_tree *tree);
void bit_tree_set(struct bit_tree *tree, int32_t index);
void bit_tree_iterate(struct bit_tree *tree, uint32_t *index_arr, int32_t len);
int32_t bit_tree_dump(struct bit_tree *tree, uint32_t *restrict index_arr, int32_t len);
