#include <emmintrin.h>
#include "metricregroup.h"
#include "libdivide.h"

void calculateGroups(uint32_t min, uint32_t max, uint64_t magicNumber, uint32_t numBuckets, uint32_t n, uint32_t* valBuf, uint32_t* docGroupBuffer) {
	struct libdivide_u32_t magic = *(struct libdivide_u32_t*)&magicNumber;
	const uint32_t minGroup = numBuckets+1;
	const uint32_t maxGroup = numBuckets+2;
	const __m128i minGroupVector = _mm_set_epi32(minGroup, minGroup, minGroup, minGroup);
	const __m128i maxGroupVector = _mm_set_epi32(maxGroup, maxGroup, maxGroup, maxGroup);
	const __m128i minVector = _mm_set_epi32(min, min, min, min);
	const __m128i maxVector = _mm_set_epi32(max, max, max, max);
	const __m128i one = _mm_set_epi32(1, 1, 1, 1);
	const __m128i negativeOne = _mm_set_epi32(-1, -1, -1, -1);
	int i;
	for (i = 0; i+3 < n; i+=4) {

		__m128i values = _mm_load_si128(valBuf+i);

		__m128i ltMin = _mm_cmplt_epi32(values, minVector);
		__m128i gteMin = _mm_xor_si128(ltMin, negativeOne);
		__m128i ltMax = _mm_cmplt_epi32(values, maxVector);
		__m128i gteMax = _mm_xor_si128(ltMax, negativeOne);

		__m128i diff = _mm_sub_epi32(values, minVector);
		__m128i quot = libdivide_u32_do_vector(diff, &magic);
		__m128i sum = _mm_add_epi32(quot, one);

		__m128i selectMin = _mm_and_si128(minGroupVector, ltMin);
		__m128i selectMax = _mm_and_si128(maxGroupVector, gteMax);

		__m128i clearMin = _mm_and_si128(sum, gteMin);
		__m128i clearMax = _mm_and_si128(clearMin, ltMax);

		__m128i result1 = _mm_or_si128(clearMax, selectMin);
		__m128i result2 = _mm_or_si128(result1, selectMax);

		_mm_store_si128(docGroupBuffer+i, result2);
	}
	for (; i < n; i++) {
		int group;
		int val = valBuf[i];
		if (val < min) {
			group = minGroup;
		} else if (val >= max) {
			group = maxGroup;
		} else {
			group = libdivide_u32_do((val - min), &magic) + 1;
		}
		docGroupBuffer[i] = group;
	}
}

uint64_t getMagicNumber(uint32_t divisor) {
	struct libdivide_u32_t magicNumber = libdivide_u32_gen(divisor);
	return *(uint64_t*)&magicNumber;
}
