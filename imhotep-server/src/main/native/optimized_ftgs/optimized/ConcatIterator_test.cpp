/*
 * merge_iterator_test.cpp
 *
 *  Created on: May 20, 2015
 *      Author: darren
 */

#include "ChainedIterator.hpp"
#include <iostream>       // std::cout

struct fubar {
    int bar;

    fubar(int i) : bar(i) {}
};

int main()
{

    std::vector<fubar> vec1;
    std::vector<fubar> vec2;
    std::vector<fubar> vec3;
    std::vector<fubar> vec4;
    std::vector<fubar> vec5;

    std::vector<std::vector<fubar>::iterator> vecs;
    std::vector<std::vector<fubar>::iterator> vec_ends;

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

    vec_ends.push_back(vec1.end());
    vec_ends.push_back(vec2.end());
    vec_ends.push_back(vec3.end());
    vec_ends.push_back(vec4.end());
    vec_ends.push_back(vec5.end());

    int32_t start_int = 1;
    int32_t end_int = 11;

    imhotep::ChainedIterator<std::vector<fubar>::iterator> my_iter(vecs.begin(),
                                                                 vecs.end(),
                                                                 vec_ends.begin(),
                                                                 vec_ends.end(),
    [start_int] (int num) {
        return fubar(-(start_int + num));
    },
    [end_int] (int num) {
        return fubar(-(end_int + num));
    });

    imhotep::ChainedIterator<std::vector<fubar>::iterator> my_iter_end;

    while(my_iter != my_iter_end) {
        std::cout << my_iter->bar << "\n";
        my_iter ++;
    }
    std::cout << my_iter->bar << "\n";

    std::cout << "complete.\n";

  return 0;
}
