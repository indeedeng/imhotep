#include "com_indeed_imhotep_local_NativeMetricRegroupInternals.h"
#include "metricregroup.h"

JNIEXPORT jlong JNICALL Java_com_indeed_imhotep_local_NativeMetricRegroupInternals_getMagicNumber(
		JNIEnv* env,
		jclass class,
		jint divisor
) {
	return getMagicNumber(divisor);
}

JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_NativeMetricRegroupInternals_calculateGroups(
		JNIEnv* env,
		jclass class,
		jint min,
		jint max,
		jlong magicNumber,
		jint numBuckets,
		jint n,
		jlong valBuf,
		jlong docGroupBuffer
) {
	calculateGroups(min, max, magicNumber, numBuckets, n, (uint32_t*)valBuf, (uint32_t*)docGroupBuffer);
}
