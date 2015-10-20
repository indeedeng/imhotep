/*
 * merge_iterator_test.cpp
 *
 *  Created on: May 20, 2015
 *      Author: darren
 */

#include <algorithm>
#include <array>
#include <vector>
#include <iostream>       // std::cout
#include <list>
#include <utility>

#include "interleaved_jiterator.hpp"
#include "jiterator.hpp"

int main()
{
    typedef std::list<int> List;

    std::array<List, 5> lists;
    for (size_t i(0); i < lists.size(); ++i) {
        for (size_t j(0); j < lists.size(); ++j) {
            lists[j].push_back(i * 5 + j);
        }
    }

    std::vector<imhotep::iterator_2_jIterator<List::iterator>> iters;
    for (size_t i(0); i < 5; ++i) {
        iters.push_back(imhotep::iterator_2_jIterator<List::iterator>(lists[i].begin(), lists[i].end()));
    }

    imhotep::InterleavedJIterator<imhotep::iterator_2_jIterator<List::iterator>> my_iter(iters.begin(), iters.end());

    int foo = -99;
    while(my_iter.hasNext()) {
        my_iter.next(foo);
        std::cout << foo << ", ";
    }

    std::cout << "\n";

    std::cout << "complete.\n";

  return 0;
}
