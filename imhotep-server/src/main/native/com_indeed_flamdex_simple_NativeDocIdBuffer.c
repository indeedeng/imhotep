#include "com_indeed_flamdex_simple_NativeDocIdBuffer.h"
#include "varintdecode.h"

JNIEXPORT jlong JNICALL Java_com_indeed_flamdex_simple_NativeDocIdBuffer_readInts(JNIEnv* env, jclass class, jlong in, jlong out, jint length) {
	return read_ints(0, (uint8_t*)in, (uint32_t*)out, length);
}

JNIEXPORT jlong JNICALL Java_com_indeed_flamdex_simple_NativeDocIdBuffer_readIntsSingle(JNIEnv* env, jclass class, jlong in, jlong out, jint length) {
	return read_ints_single(0, (uint8_t*)in, (uint32_t*)out, length);
}

JNIEXPORT void JNICALL Java_com_indeed_flamdex_simple_NativeDocIdBuffer_nativeInit(JNIEnv* env, jclass class) {
	init();
}
