#include <jni.h>

#undef  JNIEXPORT
#define JNIEXPORT               __attribute__((visibility("default")))

#include "jni/com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher.h"
#include "metrics_inverter.h"

#undef com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher_BUFFER_SIZE
#define com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher_BUFFER_SIZE 8192L
/*
 * Class:     com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher
 * Method:    nativeCacheBitsetMetricValuesMmap
 * Signature: (JIJJ)V
 */
JNIEXPORT void JNICALL Java_com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher_nativeCacheBitsetMetricValuesMmap
                                (JNIEnv *env,
                                 jclass field_cacher,
                                 jlong save_buffer,
                                 jint n_docs,
                                 jlong doc_list_addr,
                                 jlong offset)
{
    uint8_t* delta_compressed_doc_ids = (uint8_t*)doc_list_addr;
    int64_t* metrics = (int64_t*)save_buffer;

    invert_bitfield_metric(metrics, n_docs, delta_compressed_doc_ids, offset);
}

/*
 * Class:     com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher
 * Method:    nativeCacheBitsetMetricValuesInArray
 * Signature: ([JIJJ)V
 */
JNIEXPORT void JNICALL Java_com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher_nativeCacheBitsetMetricValuesInArray
                                  (JNIEnv *env,
                                   jclass field_cacher,
                                   jlongArray save_array,
                                   jint n_docs,
                                   jlong doc_list_addr,
                                   jlong offset)
{
    uint8_t* delta_compressed_doc_ids = (uint8_t*)doc_list_addr;
    jlong* metrics;
    jboolean unused;

    metrics = (*env)->GetPrimitiveArrayCritical(env, save_array, &unused);
    invert_bitfield_metric(metrics, n_docs, delta_compressed_doc_ids, offset);
    (*env)->ReleasePrimitiveArrayCritical(env, save_array, metrics, 0);
}

/*
 * Class:     com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher
 * Method:    nativeCacheCharMetricValuesMMap
 * Signature: (JI[C[IJ[JI)V
 */
JNIEXPORT void JNICALL Java_com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher_nativeCacheCharMetricValuesMMap
                                  (JNIEnv *env,
                                   jclass field_cacher,
                                   jlong save_buffer,
                                   jcharArray terms_array,
                                   jintArray n_docs_array,
                                   jlong doc_list_addr,
                                   jlongArray offsets_array,
                                   jint terms_len)
{
    uint8_t* delta_compressed_doc_ids = (uint8_t*)doc_list_addr;
    int16_t* metrics = (int16_t*)save_buffer;
    int16_t* terms;
    jint* n_docs;
    jlong* offsets;
    jboolean unused;

    terms = (*env)->GetPrimitiveArrayCritical(env, terms_array, &unused);
    n_docs = (*env)->GetPrimitiveArrayCritical(env, n_docs_array, &unused);
    offsets = (*env)->GetPrimitiveArrayCritical(env, offsets_array, &unused);
    invert_short_metric(metrics, terms, n_docs, delta_compressed_doc_ids, offsets, terms_len);
    (*env)->ReleasePrimitiveArrayCritical(env, offsets_array, offsets, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, n_docs_array, n_docs, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, terms_array, terms, JNI_ABORT);
}

/*
 * Class:     com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher
 * Method:    nativeCacheCharMetricValuesInArray
 * Signature: ([CI[C[IJ[JI)V
 */
JNIEXPORT void JNICALL Java_com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher_nativeCacheCharMetricValuesInArray
                                  (JNIEnv *env,
                                   jclass field_cacher,
                                   jcharArray save_array,
                                   jcharArray terms_array,
                                   jintArray n_docs_array,
                                   jlong doc_list_addr,
                                   jlongArray offsets_array,
                                   jint terms_len)
{
    uint8_t* delta_compressed_doc_ids = (uint8_t*)doc_list_addr;
    int16_t* metrics;
    int16_t* terms;
    jint* n_docs;
    jlong* offsets;
    jboolean unused;

    metrics = (*env)->GetPrimitiveArrayCritical(env, save_array, &unused);
    terms = (*env)->GetPrimitiveArrayCritical(env, terms_array, &unused);
    n_docs = (*env)->GetPrimitiveArrayCritical(env, n_docs_array, &unused);
    offsets = (*env)->GetPrimitiveArrayCritical(env, offsets_array, &unused);
    invert_short_metric(metrics, terms, n_docs, delta_compressed_doc_ids, offsets, terms_len);
    (*env)->ReleasePrimitiveArrayCritical(env, offsets_array, offsets, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, n_docs_array, n_docs, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, terms_array, terms, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, save_array, metrics, 0);
}

/*
 * Class:     com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher
 * Method:    nativeCacheByteMetricValuesMMap
 * Signature: (JI[B[IJ[JI)V
 */
JNIEXPORT void JNICALL Java_com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher_nativeCacheByteMetricValuesMMap
                                    (JNIEnv *env,
                                     jclass field_cacher,
                                     jlong save_buffer,
                                     jbyteArray terms_array,
                                     jintArray n_docs_array,
                                     jlong doc_list_addr,
                                     jlongArray offsets_array,
                                     jint terms_len)
{
    uint8_t* delta_compressed_doc_ids = (uint8_t*)doc_list_addr;
    int8_t* metrics = (int8_t*)save_buffer;
    jbyte* terms;
    jint* n_docs;
    jlong* offsets;
    jboolean unused;

    terms = (*env)->GetPrimitiveArrayCritical(env, terms_array, &unused);
    n_docs = (*env)->GetPrimitiveArrayCritical(env, n_docs_array, &unused);
    offsets = (*env)->GetPrimitiveArrayCritical(env, offsets_array, &unused);
    invert_byte_metric(metrics, terms, n_docs, delta_compressed_doc_ids, offsets, terms_len);
    (*env)->ReleasePrimitiveArrayCritical(env, offsets_array, offsets, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, n_docs_array, n_docs, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, terms_array, terms, JNI_ABORT);
}

/*
 * Class:     com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher
 * Method:    nativeCacheByteMetricValuesInArray
 * Signature: ([BI[B[IJ[JI)V
 */
JNIEXPORT void JNICALL Java_com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher_nativeCacheByteMetricValuesInArray
                                    (JNIEnv *env,
                                    jclass field_cacher,
                                    jbyteArray save_array,
                                    jbyteArray terms_array,
                                    jintArray n_docs_array,
                                    jlong doc_list_addr,
                                    jlongArray offsets_array,
                                    jint terms_len)
{
     uint8_t* delta_compressed_doc_ids = (uint8_t*)doc_list_addr;
     int8_t* metrics;
     jbyte* terms;
     jint* n_docs;
     jlong* offsets;
     jboolean unused;

     metrics = (*env)->GetPrimitiveArrayCritical(env, save_array, &unused);
     terms = (*env)->GetPrimitiveArrayCritical(env, terms_array, &unused);
     n_docs = (*env)->GetPrimitiveArrayCritical(env, n_docs_array, &unused);
     offsets = (*env)->GetPrimitiveArrayCritical(env, offsets_array, &unused);
     invert_byte_metric(metrics, terms, n_docs, delta_compressed_doc_ids, offsets, terms_len);
     (*env)->ReleasePrimitiveArrayCritical(env, offsets_array, offsets, JNI_ABORT);
     (*env)->ReleasePrimitiveArrayCritical(env, n_docs_array, n_docs, JNI_ABORT);
     (*env)->ReleasePrimitiveArrayCritical(env, terms_array, terms, JNI_ABORT);
     (*env)->ReleasePrimitiveArrayCritical(env, save_array, metrics, 0);
}

/*
 * Class:     com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher
 * Method:    nativeCacheShortMetricValuesMMap
 * Signature: (JI[S[IJ[JI)V
 */
JNIEXPORT void JNICALL Java_com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher_nativeCacheShortMetricValuesMMap
                                    (JNIEnv *env,
                                     jclass field_cacher,
                                     jlong save_buffer,
                                     jshortArray terms_array,
                                     jintArray n_docs_array,
                                     jlong doc_list_addr,
                                     jlongArray offsets_array,
                                     jint terms_len)
{
    uint8_t* delta_compressed_doc_ids = (uint8_t*)doc_list_addr;
    int16_t* metrics = (int16_t*)save_buffer;
    jshort* terms;
    jint* n_docs;
    jlong* offsets;
    jboolean unused;

    terms = (*env)->GetPrimitiveArrayCritical(env, terms_array, &unused);
    n_docs = (*env)->GetPrimitiveArrayCritical(env, n_docs_array, &unused);
    offsets = (*env)->GetPrimitiveArrayCritical(env, offsets_array, &unused);
    invert_short_metric(metrics, terms, n_docs, delta_compressed_doc_ids, offsets, terms_len);
    (*env)->ReleasePrimitiveArrayCritical(env, offsets_array, offsets, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, n_docs_array, n_docs, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, terms_array, terms, JNI_ABORT);
}

/*
 * Class:     com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher
 * Method:    nativeCacheShortMetricValuesInArray
 * Signature: ([SI[S[IJ[JI)V
 */
JNIEXPORT void JNICALL Java_com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher_nativeCacheShortMetricValuesInArray
                                    (JNIEnv *env,
                                     jclass field_cacher,
                                     jshortArray save_array,
                                     jshortArray terms_array,
                                     jintArray n_docs_array,
                                     jlong doc_list_addr,
                                     jlongArray offsets_array,
                                     jint terms_len)
{
    uint8_t* delta_compressed_doc_ids = (uint8_t*)doc_list_addr;
    jshort* metrics;
    jshort* terms;
    jint* n_docs;
    jlong* offsets;
    jboolean unused;

    metrics = (*env)->GetPrimitiveArrayCritical(env, save_array, &unused);
    terms = (*env)->GetPrimitiveArrayCritical(env, terms_array, &unused);
    n_docs = (*env)->GetPrimitiveArrayCritical(env, n_docs_array, &unused);
    offsets = (*env)->GetPrimitiveArrayCritical(env, offsets_array, &unused);
    invert_short_metric(metrics, terms, n_docs, delta_compressed_doc_ids, offsets, terms_len);
    (*env)->ReleasePrimitiveArrayCritical(env, offsets_array, offsets, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, n_docs_array, n_docs, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, terms_array, terms, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, save_array, metrics, 0);
}

/*
 * Class:     com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher
 * Method:    nativeCacheIntMetricValuesMMap
 * Signature: (JI[I[IJ[JI)V
 */
JNIEXPORT void JNICALL Java_com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher_nativeCacheIntMetricValuesMMap
                                    (JNIEnv *env,
                                     jclass field_cacher,
                                     jlong save_buffer,
                                     jintArray terms_array,
                                     jintArray n_docs_array,
                                     jlong doc_list_addr,
                                     jlongArray offsets_array,
                                     jint terms_len)
{
    uint8_t* delta_compressed_doc_ids = (uint8_t*)doc_list_addr;
    int32_t* metrics = (int32_t*)save_buffer;
    jint* terms;
    jint* n_docs;
    jlong* offsets;
    jboolean unused;

    terms = (*env)->GetPrimitiveArrayCritical(env, terms_array, &unused);
    n_docs = (*env)->GetPrimitiveArrayCritical(env, n_docs_array, &unused);
    offsets = (*env)->GetPrimitiveArrayCritical(env, offsets_array, &unused);
    invert_int_metric(metrics, terms, n_docs, delta_compressed_doc_ids, offsets, terms_len);
    (*env)->ReleasePrimitiveArrayCritical(env, offsets_array, offsets, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, n_docs_array, n_docs, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, terms_array, terms, JNI_ABORT);
}

/*
 * Class:     com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher
 * Method:    nativeCacheIntMetricValuesInArray
 * Signature: ([II[I[IJ[JI)V
 */
JNIEXPORT void JNICALL Java_com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher_nativeCacheIntMetricValuesInArray
                                    (JNIEnv *env,
                                     jclass field_cacher,
                                     jintArray save_array,
                                     jintArray terms_array,
                                     jintArray n_docs_array,
                                     jlong doc_list_addr,
                                     jlongArray offsets_array,
                                     jint terms_len)
{
    uint8_t* delta_compressed_doc_ids = (uint8_t*)doc_list_addr;
    jint* metrics;
    jint* terms;
    jint* n_docs;
    jlong* offsets;
    jboolean unused;

    metrics = (*env)->GetPrimitiveArrayCritical(env, save_array, &unused);
    terms = (*env)->GetPrimitiveArrayCritical(env, terms_array, &unused);
    n_docs = (*env)->GetPrimitiveArrayCritical(env, n_docs_array, &unused);
    offsets = (*env)->GetPrimitiveArrayCritical(env, offsets_array, &unused);
    invert_int_metric(metrics, terms, n_docs, delta_compressed_doc_ids, offsets, terms_len);
    (*env)->ReleasePrimitiveArrayCritical(env, offsets_array, offsets, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, n_docs_array, n_docs, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, terms_array, terms, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, save_array, metrics, 0);
}

/*
 * Class:     com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher
 * Method:    nativeCacheLongMetricValuesMMap
 * Signature: (JI[J[IJ[JI)V
 */
JNIEXPORT void JNICALL Java_com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher_nativeCacheLongMetricValuesMMap
                                    (JNIEnv *env,
                                     jclass field_cacher,
                                     jlong save_buffer,
                                     jlongArray terms_array,
                                     jintArray n_docs_array,
                                     jlong doc_list_addr,
                                     jlongArray offsets_array,
                                     jint terms_len)
{
    uint8_t* delta_compressed_doc_ids = (uint8_t*)doc_list_addr;
    int64_t* metrics = (int64_t*)save_buffer;
    jlong* terms;
    jint* n_docs;
    jlong* offsets;
    jboolean unused;

    terms = (*env)->GetPrimitiveArrayCritical(env, terms_array, &unused);
    n_docs = (*env)->GetPrimitiveArrayCritical(env, n_docs_array, &unused);
    offsets = (*env)->GetPrimitiveArrayCritical(env, offsets_array, &unused);
    invert_long_metric(metrics, terms, n_docs, delta_compressed_doc_ids, offsets, terms_len);
    (*env)->ReleasePrimitiveArrayCritical(env, offsets_array, offsets, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, n_docs_array, n_docs, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, terms_array, terms, JNI_ABORT);
}

/*
 * Class:     com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher
 * Method:    nativeCacheLongMetricValuesInArray
 * Signature: ([JI[J[IJ[JI)V
 */
JNIEXPORT void JNICALL Java_com_indeed_flamdex_fieldcache_NativeFlamdexFieldCacher_nativeCacheLongMetricValuesInArray
                                (JNIEnv *env,
                                 jclass field_cacher,
                                 jlongArray save_array,
                                 jlongArray terms_array,
                                 jintArray n_docs_array,
                                 jlong doc_list_addr,
                                 jlongArray offsets_array,
                                 jint terms_len)
{
    uint8_t* delta_compressed_doc_ids = (uint8_t*)doc_list_addr;
    jlong* metrics;
    jlong* terms;
    jint* n_docs;
    jlong* offsets;
    jboolean unused;

    metrics = (*env)->GetPrimitiveArrayCritical(env, save_array, &unused);
    terms = (*env)->GetPrimitiveArrayCritical(env, terms_array, &unused);
    n_docs = (*env)->GetPrimitiveArrayCritical(env, n_docs_array, &unused);
    offsets = (*env)->GetPrimitiveArrayCritical(env, offsets_array, &unused);
    invert_long_metric(metrics, terms, n_docs, delta_compressed_doc_ids, offsets, terms_len);
    (*env)->ReleasePrimitiveArrayCritical(env, offsets_array, offsets, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, n_docs_array, n_docs, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, terms_array, terms, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, save_array, metrics, 0);
}

