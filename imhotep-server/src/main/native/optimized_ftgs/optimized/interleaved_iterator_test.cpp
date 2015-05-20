/*
 * merge_iterator_test.cpp
 *
 *  Created on: May 20, 2015
 *      Author: darren
 */

#include "interleaved_iterator.hpp"
#include <iostream>       // std::cout

int main()
{

    std::vector<int> vec1;
    std::vector<int> vec2;
    std::vector<int> vec3;
    std::vector<int> vec4;
    std::vector<int> vec5;

    std::vector<std::vector<int>::iterator> vecs;

    for (int i = 0; i < 5; i ++) {
        vec1.push_back(i * 5);
        vec2.push_back(i * 5 + 1);
        vec3.push_back(i * 5 + 2);
        vec4.push_back(i * 5 + 3);
        vec5.push_back(i * 5 + 4);
    }

    vecs.push_back(vec1.begin());
    vecs.push_back(vec2.begin());
    vecs.push_back(vec3.begin());
    vecs.push_back(vec4.begin());
    vecs.push_back(vec5.begin());

    imhotep::interleaved_iterator<std::vector<int>::iterator> my_iter(vecs.begin(), vecs.end());
    imhotep::interleaved_iterator<std::vector<int>::iterator> my_iter_end;

    while(my_iter != my_iter_end) {
        std::cout << *my_iter << ", ";
        my_iter ++;
    }

    std::cout << "\n";

    std::cout << "complete.\n";

  return 0;
}
