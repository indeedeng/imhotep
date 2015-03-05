#include "test_utils.h"

#include <array>
#include <cstdint>
#include <iostream>
#include <emmintrin.h>
#include <smmintrin.h>
#include <tmmintrin.h>
#include <pmmintrin.h>

using namespace std;

template <typename int_t>
void dump(ostream& os, int_t value) {
  typedef array<uint8_t, sizeof(int_t)> byte_array;
  // char* begin(reinterpret_cast<char*>(&value));
  const byte_array& bytes(*reinterpret_cast<byte_array*>(&value));
  vector<uint8_t> byte_vec(bytes.begin(), bytes.end());
  os << byte_vec;
}

int main(int arg, char* argv[])
{
  int64_t high(0x7766554433221100);
  int64_t low(0xaabbccddeeff9988);

  __m128i src(_mm_set_epi64x(low, high));
  __m128i dst(_mm_set_epi64x(0, 0));

  cout << "before..." << endl;
  cout << "high: "; dump(cout, high); cout << endl;
  cout << " low: "; dump(cout, low);  cout << endl;
  cout << " src: "; dump(cout, src);  cout << endl;
  cout << " dst: "; dump(cout, dst);  cout << endl;

  __m128i mask(_mm_setr_epi8(15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0));
  dst = _mm_shuffle_epi8(src, mask);

  cout << "after..." << endl;
  cout << " src: "; dump(cout, src);  cout << endl;
  cout << " dst: "; dump(cout, dst);  cout << endl;
}
