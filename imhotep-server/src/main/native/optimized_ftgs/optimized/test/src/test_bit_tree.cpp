#include <bitset>
#include <cstdint>
#include <iostream>

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>

#define restrict __restrict__
extern "C" {
#include "bit_tree.h"
}

using namespace std;

class Random
{
    boost::random::mt19937 _generator;
public:
    int operator()(int n) {
        boost::random::uniform_int_distribution<> dist(0, n);
        return dist(_generator);
    }
};

int main(int argc, char* argv[])
{
    typedef bitset<0x40000000> BitSet;
    BitSet* bs(new BitSet());

    Random random;
    for (int i(0); i < 30; ++i) {
        for (int density(1); density <= 16; ++density) {

            const int size(random(1<<i) + 1);
            bs->reset();
            struct bit_tree tree;
            bit_tree_init(&tree, size);

            size_t n_entries(size/(1<<density));
            for (size_t j(0); j < n_entries; ++j) {
                const int value(random(size));
                bs->set(value);
                bit_tree_set(&tree, value);
            }

            uint32_t* entries = (uint32_t *) calloc(n_entries, sizeof(uint32_t));
            const size_t result(bit_tree_dump(&tree, entries, n_entries));
            for (size_t j(0); j < result; ++j) {
                const uint32_t entry(entries[j]);
                assert(bs->test(entry));
            }

            free(entries);
            bit_tree_destroy(&tree);
        }
    }

    delete bs;
}
