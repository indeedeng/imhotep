#include <jni.h>

#undef  JNIEXPORT
#define JNIEXPORT               __attribute__((visibility("default")))

#include "com_indeed_imhotep_local_MultiRegroupInternals.h"
#include "com_indeed_imhotep_local_MTImhotepLocalMultiSession.h"
#include "imhotep_native.h"
#include "local_session.h"
#include "remote_output.h"
#include "table.h"
#include "varintdecode.h"

/*
 * Class:     com_indeed_imhotep_local_MultiRegroupInternals
 * Method:    nativeRemapDocsInTargetGroups
 * Signature: (J[BJI[II)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_MultiRegroupInternals_nativeRemapDocsInTargetGroups__J_3BJI_3II
(JNIEnv *env, jclass clazz,
 long native_shard_data_ptr, jbyteArray results,
 jlong doc_list_address, jint n_docs,
 jintArray remappings,
 jint placeholder_group)
{
    jboolean unused           = 0;
    jbyte*   results_array    = (*env)->GetPrimitiveArrayCritical(env, results, &unused);
    jint*    remappings_array = (*env)->GetPrimitiveArrayCritical(env, remappings, &unused);

    int status = remap_docs_in_target_groups_int8_t((packed_table_t*) native_shard_data_ptr,
                                                    results_array,
                                                    (uint8_t*) doc_list_address, n_docs,
                                                    remappings_array,
                                                    placeholder_group);

    (*env)->ReleasePrimitiveArrayCritical(env, remappings, remappings_array, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, results,    results_array,    0);

    if (status != 0) {
        jclass exClass = (*env)->FindClass(env, "java/lang/IllegalArgumentException");
        (*env)->ThrowNew(env, exClass,
                         "Regrouping on a multi-valued field doesn't work correctly so the "
                         "operation is rejected.");
    }
}

/*
 * Class:     com_indeed_imhotep_local_MultiRegroupInternals
 * Method:    nativeRemapDocsInTargetGroups
 * Signature: (J[CJI[II)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_MultiRegroupInternals_nativeRemapDocsInTargetGroups__J_3CJI_3II
(JNIEnv *env, jclass clazz,
 long native_shard_data_ptr, jcharArray results,
 jlong doc_list_address, jint n_docs,
 jintArray remappings,
 jint placeholder_group)
{
    jboolean unused           = 0;
    jchar*   results_array    = (*env)->GetPrimitiveArrayCritical(env, results, &unused);
    jint*    remappings_array = (*env)->GetPrimitiveArrayCritical(env, remappings, &unused);

    int status = remap_docs_in_target_groups_uint16_t((packed_table_t*) native_shard_data_ptr,
                                                      results_array,
                                                      (uint8_t*) doc_list_address, n_docs,
                                                      remappings_array,
                                                      placeholder_group);

    (*env)->ReleasePrimitiveArrayCritical(env, remappings, remappings_array, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, results,    results_array,    0);

    if (status != 0) {
        jclass exClass = (*env)->FindClass(env, "java/lang/IllegalArgumentException");
        (*env)->ThrowNew(env, exClass,
                         "Regrouping on a multi-valued field doesn't work correctly so the "
                         "operation is rejected.");
    }
}

/*
 * Class:     com_indeed_imhotep_local_MultiRegroupInternals
 * Method:    nativeRemapDocsInTargetGroups
 * Signature: (J[IJI[II)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_MultiRegroupInternals_nativeRemapDocsInTargetGroups__J_3IJI_3II
(JNIEnv *env, jclass clazz,
 long native_shard_data_ptr, jintArray results,
 jlong doc_list_address, jint n_docs,
 jintArray remappings,
 jint placeholder_group)
{
    jboolean unused           = 0;
    jint*    results_array    = (*env)->GetPrimitiveArrayCritical(env, results, &unused);
    jint*    remappings_array = (*env)->GetPrimitiveArrayCritical(env, remappings, &unused);

    int status = remap_docs_in_target_groups_int32_t((packed_table_t*) native_shard_data_ptr,
                                                     results_array,
                                                     (uint8_t*) doc_list_address, n_docs,
                                                     remappings_array,
                                                     placeholder_group);

    (*env)->ReleasePrimitiveArrayCritical(env, remappings, remappings_array, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, results,    results_array,    0);

    if (status != 0) {
        jclass exClass = (*env)->FindClass(env, "java/lang/IllegalArgumentException");
        (*env)->ThrowNew(env, exClass,
                         "Regrouping on a multi-valued field doesn't work correctly so the "
                         "operation is rejected.");
    }
}


/*
 * Class:     com_indeed_imhotep_local_MTImhotepLocalMultiSession
 * Method:    nativeInit
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_MTImhotepLocalMultiSession_nativeInit
(JNIEnv *env, jclass mt_local_session_class)
{
    simdvbyteinit();
}


/*
 * Class:     com_indeed_imhotep_local_PackedTableView
 * Method:    nativeBind
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_PackedTableView_nativeBind
(JNIEnv *env, jobject packedTableView, jlong nativeShardDataPtr)
{
    packed_table_t* packed_table = (packed_table_t*) nativeShardDataPtr;
    jclass   clazz          = (*env)->GetObjectClass(env, packedTableView);
    jfieldID tableDataPtrID = (*env)->GetFieldID(env, clazz, "tableDataPtr", "J");
    jfieldID rowSizeID      = (*env)->GetFieldID(env, clazz, "rowSizeBytes", "I");
    (*env)->SetLongField(env, packedTableView, tableDataPtrID, (jlong) packed_table->data);
    (*env)->SetIntField(env, packedTableView, rowSizeID, (jint) packed_table->row_size_bytes);
}

