#ifndef TEST_UTILS_H
#define TEST_UTILS_H

#include <array>
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
    while (value > 127) {
      *result = static_cast<uint8_t>(value & 127) | 128;
      ++result;
      value >>= 7;
    }
    *result = static_cast<uint8_t>(value & 127);
    ++result;
    ++current;
  }
}

template <typename iterator, typename buffer_t>
void doc_ids_encode(iterator begin, iterator end, buffer_t& out)
{
  std::vector<uint32_t> deltas;
  uint32_t value(0);
  for (iterator current(begin); current != end; ++current) {
    deltas.push_back(*current - value);
    value = *current;
  }
  varint_encode(deltas.begin(), deltas.end(), out);
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
