#include <algorithm>
#include <iostream>
#include <limits>

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>

#include "string_term_id_pool.hpp"

using namespace std;
using namespace imhotep;

class Random
{
    boost::random::mt19937 _generator;
public:
    int operator()(int n) {
        boost::random::uniform_int_distribution<> dist(0, n);
        return dist(_generator);
    }
};

static Random rng;

string random_string(size_t length)
{
    auto randchar = []() -> char
        {
            const char charset[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
            const size_t max_index = (sizeof(charset) - 1);
            return charset[rng(numeric_limits<int>::max()) % max_index ];
        };
    string result(length, 0);
    generate_n(result.begin(), length, randchar);
    return result;
}

void check(StringTermIdPool& pool, const vector<pair<string, StringTermId>>& pairs) {
    for (auto pair: pairs) {
        if (pool.intern(pair.first) != pair.second) {
            cerr << "intern failure: " << pair.first << " " << pair.second << endl;
        }
        if (pool(pair.second) != pair.first) {
            cerr << "lookup failure: " << pair.first << " " << pair.second << endl;
        }
    }
}

void test_string_term_id_pool()
{
    StringTermIdPool pool;

    vector<pair<string, StringTermId>> pairs;
    for (size_t count(0); count < numeric_limits<uint16_t>::max(); ++count) {
        const string id(random_string(rng(128)));
        const StringTermId atom(pool.intern(id));
        pairs.push_back(make_pair(id, atom));
    }
    check(pool, pairs);

    for (auto pair: pairs) pool.intern(pair.first);
    check(pool, pairs);
}

int main(int argc, char *argv[])
{
    test_string_term_id_pool();
}
