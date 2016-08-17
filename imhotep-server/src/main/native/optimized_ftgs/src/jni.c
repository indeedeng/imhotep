#include <jni.h>

#undef  JNIEXPORT
#define JNIEXPORT               __attribute__((visibility("default")))

#include "jni/com_indeed_imhotep_local_MultiRegroupInternals.h"
#include "jni/com_indeed_imhotep_local_MTImhotepLocalMultiSession.h"
#include "imhotep_native.h"
#include "local_session.h"
#include "remote_output.h"
#include "table.h"
#include "varintdecode.h"

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
    packed_table_ptr packed_table = (packed_table_ptr) nativeShardDataPtr;
    jclass   clazz          = (*env)->GetObjectClass(env, packedTableView);
    jfieldID tableDataPtrID = (*env)->GetFieldID(env, clazz, "tableDataPtr", "J");
    jfieldID rowSizeID      = (*env)->GetFieldID(env, clazz, "rowSizeBytes", "I");
    (*env)->SetLongField(env, packedTableView, tableDataPtrID, (jlong) packed_table->data);
    (*env)->SetIntField(env, packedTableView, rowSizeID, (jint) packed_table->row_size_bytes);
}

