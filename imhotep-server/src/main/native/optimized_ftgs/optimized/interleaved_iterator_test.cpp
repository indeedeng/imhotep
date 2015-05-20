/*
 * merge_iterator_test.cpp
 *
 *  Created on: May 20, 2015
 *      Author: darren
 */

#include <array>
#include <iostream>       // std::cout
#include <list>
#include <utility>

#include "interleaved_iterator.hpp"

int main()
{
    typedef std::list<int> List;

    std::array<List, 5> lists;
    for (size_t i(0); i < lists.size(); ++i) {
        for (size_t j(0); j < lists.size(); ++j) {
            lists[j].push_back(i * 5 + j);
        }
    }

    typedef std::pair<List::iterator, List::iterator> PairOfIt;
    std::array<PairOfIt, 5> pairs;
    for (size_t i(0); i < pairs.size(); ++i) {
        pairs[i] = PairOfIt(lists[i].begin(), lists[i].end());
    }

    imhotep::interleaved_iterator<List::iterator>           my_iter(pairs.begin(), pairs.end());
    imhotep::interleaved_iterator<std::list<int>::iterator> my_iter_end;

    while(my_iter != my_iter_end) {
        std::cout << *my_iter << ", ";
        my_iter ++;
    }

    std::cout << "\n";

    std::cout << "complete.\n";

  return 0;
}
