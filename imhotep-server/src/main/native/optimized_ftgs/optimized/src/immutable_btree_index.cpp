#include <cstdint>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>

#include "btree/data_block.hpp"
#include "btree/header.hpp"
#include "btree/index_block.hpp"
#include "btree/int.hpp"
#include "btree/key_value.hpp"
#include "btree/string.hpp"
#include "mmapped_file.hpp"

using namespace imhotep;
using namespace imhotep::btree;

typedef KeyValue<String, Long> IndexKV;

void dump(size_t level, const char* begin, const IndexBlock<String>& block, size_t indent=0) {
    for (size_t count(0); count < indent; ++count)
        std::cout << ' ';
    std::cout << block << std::endl;
    if (level > 0) {
        --level;
        for (size_t index(0); index < block.length(); ++index) {
            const IndexKV kv(block.key_value(index));
            if (level > 0) {
                const IndexBlock<String> ib(begin + kv.value()());
                dump(level, begin, ib, indent + 3);
            }
            else {
                const DataBlock<String, LongPair> db(begin + kv.value()());
                for (size_t count(0); count < indent + 3; ++count)
                    std::cout << ' ';
                std::cout << db << std::endl;
            }
        }
    }
}

int main(int argc, char* argv[]) {
    std::string fname(argv[1]);
    const MMappedFile index(fname);
    const Header header(index.end() - Header::length(), index.end());
    // !@# ulimately puke if has_deletions is true?
    std::cout << header << std::endl;

    IndexBlock<String> root_block(index.begin() + header.root_index_start_address());
    // dump(header.index_levels(), index.begin(), root_block);

    const std::vector<char> encoded(String::encode(argv[2]));
    const String            key(encoded.data());

    KeyValue<String, LongPair> result(root_block.floor_kv<LongPair>(index.begin(), key,
                                                                    size_t(header.index_levels())));
    std::cout << "result: " << result << std::endl;
}
