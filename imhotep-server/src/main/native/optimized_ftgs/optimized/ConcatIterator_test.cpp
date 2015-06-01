/*
 * merge_iterator_test.cpp
 *
 *  Created on: May 20, 2015
 *      Author: darren
 */
#include <utility>
#include "chained_iterator.hpp"
#include "jiterator.hpp"
#include <iostream>       // std::cout

bool flag = false;

struct fubar {
    int bar;

    fubar(int i) : bar(i)
    { if (flag) std::cout << "created " << i << "\n"; }

    ~fubar()
    { if (flag) std::cout << "destroyed " << bar << "\n"; }
};

int main()
{

    std::vector<fubar> vec1;
    std::vector<fubar> vec2;
    std::vector<fubar> vec3;
    std::vector<fubar> vec4;
    std::vector<fubar> vec5;

    std::vector<imhotep::iterator_2_jIterator<std::vector<fubar>::iterator>> vecs;

    for (int i = 0; i < 5; i ++) {
        vec1.push_back(i * 5);
        vec2.push_back(i * 5 + 1);
        vec3.push_back(i * 5 + 2);
        vec4.push_back(i * 5 + 3);
        vec5.push_back(i * 5 + 4);
    }

    vecs.push_back(imhotep::iterator_2_jIterator<std::vector<fubar>::iterator>(vec1.begin(), vec1.end()));
    vecs.push_back(imhotep::iterator_2_jIterator<std::vector<fubar>::iterator>(vec2.begin(), vec2.end()));
    vecs.push_back(imhotep::iterator_2_jIterator<std::vector<fubar>::iterator>(vec3.begin(), vec3.end()));
    vecs.push_back(imhotep::iterator_2_jIterator<std::vector<fubar>::iterator>(vec4.begin(), vec4.end()));
    vecs.push_back(imhotep::iterator_2_jIterator<std::vector<fubar>::iterator>(vec5.begin(), vec5.end()));

    int32_t start_int = 1;
    int32_t end_int = 11;

    std::vector<imhotep::iterator_2_jIterator<std::vector<fubar>::iterator>> empty_vec;

    imhotep::ChainedIterator< imhotep::iterator_2_jIterator<std::vector<fubar>::iterator>,
                              imhotep::iterator_2_jIterator<std::vector<fubar>::iterator> >
        my_iter(vecs,
                empty_vec,
              [start_int] (int num) {
                  return fubar(-(start_int + num));
              },
              [end_int] (int num) {
                  return fubar(-(end_int + num));
              },
              []() {
                  return fubar(1313);
              });

    flag = true;

    fubar buddy(-99);
    while(my_iter.hasNext()) {
        my_iter.next(buddy);

        std::cout << buddy.bar << "\n";
    }
//    std::cout << buddy.bar << "\n";

    std::cout << "complete.\n";

    flag = false;

    return 0;
}
