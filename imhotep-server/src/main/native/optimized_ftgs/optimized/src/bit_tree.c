#include <emmintrin.h>
#include <stdlib.h>
#include <string.h>
#include "bit_tree.h"

#define ALIGNED_ALLOC(alignment, size) ((alignment) < (size)) ? aligned_alloc(alignment,size) : aligned_alloc(alignment,alignment);


static uint32_t top_bit(const int64_t x)
{
    return (sizeof(x) * 8 - 1) - __builtin_clzl(x);
}


void bit_tree_init(struct bit_tree *tree, int32_t size)
{
    tree->size = size;
    long base_len = top_bit(size - 1) / 6 + 1;  /* log base 64 of size, rounded up */
    tree->depth = base_len;
    tree->level_size = calloc(tree->depth, sizeof(*tree->level_size));
    tree->len = 0;
    for (int i = 0; i < tree->depth; i++) {
        size = (size + 63) / 64;
        tree->len += 1 << top_bit(size);
        tree->level_size[i] = 1 << top_bit(size);
    }
    tree->bitsets = ALIGNED_ALLOC(64, sizeof(uint64_t) * tree->len);
    memset(tree->bitsets, 0 , sizeof(uint64_t) * tree->len);
}

void bit_tree_destroy(struct bit_tree *tree)
{
    free(tree->bitsets);
}

void bit_tree_set(const struct bit_tree * restrict tree, uint32_t idx)
{
    uint32_t offset = 0;

    for (int i = 0; i < tree->depth; i++) {
        const uint32_t nextIndex = idx >> 6;
        tree->bitsets[offset + nextIndex] |= 1L << (idx & 0x3F);
        offset += tree->level_size[i];
        idx = nextIndex;
    }
}

void bit_tree_iterate(struct bit_tree *tree, uint32_t *idx_arr, int32_t len)
{

    
}

int32_t bit_tree_dump(const struct bit_tree * restrict tree,
                      uint32_t* restrict idx_arr,
                      const int32_t len)
{
    uint32_t index = 0;
    int32_t count = 0;
    const uint32_t depth = tree->depth;
    int32_t level = depth - 1;
    uint32_t root = tree->len - 1;
    uint32_t offset = root;

    while (1) {
        uint64_t updated_index = 0;
        uint64_t empty = (tree->bitsets[offset + index] == 0);
        uint64_t empty_mask = -empty;
        uint64_t depth_not_zero = (offset != 0);
        uint64_t depth_not_zero_mask = -depth_not_zero;
    
        if (empty && (offset == root)) {
            return count;
        }
    
        uint64_t lsb = tree->bitsets[offset + index] & -tree->bitsets[offset + index];
        tree->bitsets[offset + index] ^= lsb;
        
        int depth_inc = ((empty) ? 1 : -1) & depth_not_zero_mask;   /* gcc knows this trick better than you */
        // if (empty) {
        //     offset += 1 << (6 * (tree->depth - 1 - depth));
        // } else {
        //     offset -= 1 << (6 * (tree->depth - 1 - depth - 1));
        // }
        offset += tree->level_size[level - !empty] * depth_inc;
        level += depth_inc;

        uint32_t index_up = index >> 6;
        uint32_t index_down = (index << 6) + top_bit(lsb);
        updated_index = (empty_mask & index_up) + ((~empty_mask) & index_down);
        index = (depth_not_zero_mask & updated_index) + ((~depth_not_zero_mask) & index);
        idx_arr[count] = index_down;  /* keeps overwriting the same loc until depth == 0 */

        count += !depth_not_zero;
    }
}
