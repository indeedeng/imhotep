#ifndef TEST_UTILS_H
#define TEST_UTILS_H

#ifndef restrict
#define restrict __restrict__
#endif

#include <algorithm>
#include <array>
#include <cassert>
#include <iostream>
#include <iterator>
#include <vector>

template <typename int_t = int, typename iterator, typename buffer_t>
void varint_encode(iterator begin, iterator end, buffer_t& out)
{
  std::back_insert_iterator<buffer_t> result(std::back_inserter(out));

  iterator current(begin);
  while (current != end) {
    int_t value(*current);

    if (value < 0) {
      *result++ = ((value&0x7F) | 0x80);
      *result++ =(((value>>7)&0x7F) | 0x80);
      *result++ =(((value>>14)&0x7F) | 0x80);
      *result++ =(((value>>21)&0x7F) | 0x80);
      *result++ =(((value>>28)&0x7F) | 0x80);
    }
    else if (value < 1 << 7) {
      *result++ = value;
    }
    else if (value < 1 << 14) {
      *result++ =((value&0x7F) | 0x80);
      *result++ =(value>>7);
    }
    else if (value < 1 << 21) {
      *result++ =((value&0x7F) | 0x80);
      *result++ =(((value>>7)&0x7F) | 0x80);
      *result++ =(value>>14);
    }
    else if (value < 1 << 28) {
      *result++ =((value&0x7F) | 0x80);
      *result++ =(((value>>7)&0x7F) | 0x80);
      *result++ =(((value>>14)&0x7F) | 0x80);
      *result++ =(value>>21);
    }
    else {
      *result++ =((value&0x7F) | 0x80);
      *result++ =(((value>>7)&0x7F) | 0x80);
      *result++ =(((value>>14)&0x7F) | 0x80);
      *result++ =(((value>>21)&0x7F) | 0x80);
      *result++ =(value>>28);
    }
    ++current;
  }
}

template <class T, size_t N>
std::ostream& operator<<(std::ostream& os, const std::array<T, N>& items)
{
  for (typename std::array<T, N>::const_iterator it(items.begin()); it != items.end(); ++it) {
    if (it != items.begin()) os << " ";
    os << *it;
  }
  return os;
}

template <class T>
std::ostream& operator<<(std::ostream& os, const std::vector<T>& items)
{
  for (typename std::vector<T>::const_iterator it(items.begin()); it != items.end(); ++it) {
    if (it != items.begin()) os << " ";
    os << *it;
  }
  return os;
}

template <>
std::ostream& operator<<<uint8_t>(std::ostream& os, const std::vector<uint8_t>& items)
{
	static char digits[16] = { '0', '1', '2', '3', '4', '5', '6', '7',
														 '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

  for (typename std::vector<uint8_t>::const_iterator it(items.begin()); it != items.end(); ++it) {
    if (it != items.begin()) os << " ";
    std::array<char, 3> hex = { { digits[*it >> 4], digits[*it & 0x0f], '\0' } };
    os << hex.data();
  }
  return os;
}

#endif
