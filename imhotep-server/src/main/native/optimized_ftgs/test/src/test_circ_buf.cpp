/** @file test_varintdecode.cpp
    
    Rudimentary sanity check of circ_buf.
 */
extern "C" {
#include "circ_buf.h" 
}

#include "test_utils.h"

#include <iostream>

using namespace std;

int main(int argc, char* argv[])
{
  int status(EXIT_SUCCESS);

  constexpr int32_t n_elements(128);

  struct circular_buffer_int * buffer(circular_buffer_int_alloc(n_elements * 2));

  for (uint32_t value(0); value < n_elements; ++value) {
    circular_buffer_int_put(buffer, value);
  }

  for (uint32_t expected(0); expected < n_elements; ++expected) {
    const uint32_t actual(circular_buffer_int_get(buffer));
    if (actual != expected) {
      cerr << "FAIL expected: " << expected << " actual: " << actual << endl;
      status = EXIT_FAILURE;
    }
  }

  circular_buffer_int_cleanup(buffer);

  return status;
}
