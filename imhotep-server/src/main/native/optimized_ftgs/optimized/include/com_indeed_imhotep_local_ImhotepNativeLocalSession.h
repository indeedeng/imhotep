/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_indeed_imhotep_local_ImhotepNativeLocalSession */

#ifndef _Included_com_indeed_imhotep_local_ImhotepNativeLocalSession
#define _Included_com_indeed_imhotep_local_ImhotepNativeLocalSession
#ifdef __cplusplus
extern "C" {
#endif
#undef com_indeed_imhotep_local_ImhotepNativeLocalSession_MAX_NUMBER_STATS
#define com_indeed_imhotep_local_ImhotepNativeLocalSession_MAX_NUMBER_STATS 64L
#undef com_indeed_imhotep_local_ImhotepNativeLocalSession_BUFFER_SIZE
#define com_indeed_imhotep_local_ImhotepNativeLocalSession_BUFFER_SIZE 2048L

/*
 * Class:     com_indeed_imhotep_local_ImhotepNativeLocalSession
 * Method:    nativeGetRules
 * Signature: ([Lcom/indeed/imhotep/GroupMultiRemapRule;)J
 */
JNIEXPORT jlong JNICALL Java_com_indeed_imhotep_local_ImhotepNativeLocalSession_nativeGetRules
  (JNIEnv *, jclass, jobjectArray);

/*
 * Class:     com_indeed_imhotep_local_ImhotepNativeLocalSession
 * Method:    nativeReleaseRules
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_ImhotepNativeLocalSession_nativeReleaseRules
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_indeed_imhotep_local_ImhotepNativeLocalSession
 * Method:    nativeRegroup
 * Signature: (JZ)I
 */
JNIEXPORT jint JNICALL Java_com_indeed_imhotep_local_ImhotepNativeLocalSession_nativeRegroup
  (JNIEnv *, jclass, jlong, jboolean);

#ifdef __cplusplus
}
#endif
#endif
