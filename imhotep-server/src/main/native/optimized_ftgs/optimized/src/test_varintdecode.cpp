/** @file test_varintdecode.cpp
    
    Rudimentary sanity check of varintdecode functionality.
 */
#include "test_utils.h"
#include "varintdecode.h"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <vector>

using namespace std;

int main(int argc, char* argv[])
{
  int status(EXIT_SUCCESS);
  constexpr size_t count = 100;

  array<uint8_t, count> deltas;
  fill(deltas.begin(), deltas.end(), 1);
  deltas[0] = 0;

  array<uint32_t, count> expected;
  for (size_t i(0); i < count; ++i) { expected[i] = i; }

  vector<uint8_t> buffer;
  varint_encode(deltas.begin(), deltas.end(), buffer);

  array<uint32_t, count> results;
  size_t consumed(masked_vbyte_read_loop_delta(buffer.data(), results.data(), count, 0));

  if (consumed != buffer.size()) {
    cout << "FAIL: bytes consumed mismatch -- expected: " << buffer.size()
         << " consumed: " << consumed
         << endl;
    status = EXIT_FAILURE;
  }

  if (expected != results) {
    cout << "FAIL: roundtrip coding failed"
         << " expected: " << expected
         << "  results: " << results
         << endl;
    status = EXIT_FAILURE;
  }

  return status;
}
