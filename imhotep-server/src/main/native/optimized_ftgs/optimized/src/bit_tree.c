#include <emmintrin.h>
#include <popcntintrin.h>
#include <stdlib.h>
#include <string.h>
#include "bit_tree.h"

#ifdef _SANITIZE_
#define ALIGNED_ALLOC(alignment, size) malloc(size);
#else
#define ALIGNED_ALLOC(alignment, size) (((alignment) < (size)) ? aligned_alloc(alignment,size) : aligned_alloc(alignment,alignment));
#endif

static int32_t log2_of_size(int32_t size) {
    return 31 - __builtin_clz(size);
}

struct bit_tree * bit_tree_create(int32_t size)
{
    struct bit_tree *tree;
    const size_t length = log2_of_size(size - 1) / 6 + 1;

    tree = calloc(1, sizeof(struct bit_tree));
    tree->bitsets = calloc(length, sizeof(int64_t*));
    for (size_t i = 0; i < length; ++i) {
        size = (size+63)/64;
        tree->bitsets[i] = calloc(size, sizeof(int64_t));
    }
    tree->depth = length - 1;

    return tree;
}

void bit_tree_destroy(struct bit_tree *tree)
{
    const size_t length = tree->depth + 1;
    for (size_t i = 0; i < length; ++i) {
        free(tree->bitsets[i]);
    }
    free(tree->bitsets);
    free(tree);
}

inline void bit_tree_set(struct bit_tree *tree, int32_t index)
{
    const size_t length = tree->depth + 1;
    for (size_t i = 0; i < length; ++i) {
        const int32_t next_index = index >> 6;
        tree->bitsets[i][next_index] |= 1L << (index & 0x3f);
        index = next_index;
    }
}

void bit_tree_iterate(struct bit_tree *tree, uint32_t *index_arr, int32_t len)
{ }

inline int32_t bit_tree_dump(struct bit_tree *tree, uint32_t *restrict index_arr, int32_t len)
{
    int count = 0;
    int index = 0;
    int depth = tree->depth;
    while (1) {
        while (tree->bitsets[depth][index] == 0) {
            if (depth == tree->depth) return count;
            ++depth;
            index = index >> 6;
        }
        while (depth != 0) {
            const int64_t lsb = tree->bitsets[depth][index] & -tree->bitsets[depth][index];
            tree->bitsets[depth][index] ^= lsb;
            --depth;
            index = (index << 6) + _mm_popcnt_u64(lsb - 1);
        }
        while (tree->bitsets[0][index] != 0) {
            const int64_t lsb = tree->bitsets[0][index] & -tree->bitsets[0][index];
            tree->bitsets[0][index] ^= lsb;
            index_arr[count++] = (index << 6) + _mm_popcnt_u64(lsb - 1);
        }
        if (tree->depth == 0) return count;
        depth = 1;
        index = index >> 6;
    }
    return count;
}
