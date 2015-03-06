#include <jni.h>
#include "imhotep_native.h"
#include "local_session.h"

/*
 * Class:     com_indeed_imhotep_local_NativeFTGSWorker
 * Method:    native_init
 * Signature: (III[II)J
 */
JNIEXPORT jlong JNICALL Java_com_indeed_imhotep_local_NativeFTGSWorker_native_1init
  (JNIEnv *java_env, jclass class, jint id, jint n_groups, jint n_metrics,
   jintArray socket_fds, jint len)
{
	struct worker_desc *worker;
	jint *fds;
	jboolean madeCopy;

	fds = (*java_env)->GetPrimitiveArrayCritical(java_env, socket_fds, &madeCopy);
	worker = calloc(1, sizeof(struct worker_desc));
	worker_init(worker,id, n_groups, n_metrics, fds, len);
	(*java_env)->ReleasePrimitiveArrayCritical(java_env, socket_fds, fds, JNI_ABORT);

	return (jlong)worker;
}



/*
 * Class:     com_indeed_imhotep_local_NativeFTGSWorker
 * Method:    native_session_create
 * Signature: (III)J
 */
JNIEXPORT jlong JNICALL Java_com_indeed_imhotep_local_NativeFTGSWorker_native_1session_1create
  (JNIEnv *env, jclass class, jint n_groups, jint n_metrics, jbyteArray stat_order, jint n_shards)
{
	struct session_desc *session;
	
	session = calloc(1, sizeof(struct session_desc));
	uint8_t* order = (*env)->GetPrimitiveArrayCritical(env, stat_order, 0);
	session_init(session, n_groups, n_metrics, order, n_shards);
	(*env)->ReleasePrimitiveArrayCritical(env, stat_order, order, 0);

	return (jlong)session;
}

JNIEXPORT jint JNICALL Java_com_indeed_imhotep_local_NativeFTGSWorker_native_1run_1int_1tgs_1pass
  (JNIEnv *java_env, jclass class, jlong worker_addr, jlong session_addr, jint int_term, 
  jlongArray slice_offsets_arr, jintArray docs_per_slice_arr, jintArray shard_ids_arr, 
  jint num_shards, jint socket_fd)
{
	struct worker_desc *worker;
	struct session_desc *session;
	jlong *slice_offsets;
	jint *docs_per_slice;
	jint *shard_ids;
	jboolean madeCopy;
	
	worker = (struct worker_desc *)worker_addr;
  	session = (struct session_desc *)session_addr;

	slice_offsets = (*java_env)->GetPrimitiveArrayCritical(java_env, slice_offsets_arr, &madeCopy);
	docs_per_slice = (*java_env)->GetPrimitiveArrayCritical(java_env, docs_per_slice_arr, &madeCopy);
	shard_ids = (*java_env)->GetPrimitiveArrayCritical(java_env, shard_ids_arr, &madeCopy);

	int err = run_tgs_pass(worker,
	                       session,
	                       TERM_TYPE_INT,
	                       int_term,
	                       NULL,
	                       slice_offsets,
	                       docs_per_slice,
	                       shard_ids,
	                       num_shards,
	                       socket_fd);
	
	(*java_env)->ReleasePrimitiveArrayCritical(java_env, shard_ids_arr, shard_ids, JNI_ABORT);
	(*java_env)->ReleasePrimitiveArrayCritical(java_env, docs_per_slice_arr, docs_per_slice, JNI_ABORT);
	(*java_env)->ReleasePrimitiveArrayCritical(java_env, slice_offsets_arr, slice_offsets, JNI_ABORT);

	return (jint)err;
}

static jlong getNativeShardDataPtr(JNIEnv *java_env, jobject instance)
{
  jclass   clazz    = (*java_env)->GetObjectClass(java_env, instance);
  jfieldID field_id = (*java_env)->GetFieldID(java_env, clazz, "MultiCache.this.nativeShardDataPtr", "L");
  return (*java_env)->GetLongField(java_env, instance, field_id);
}

/*
 * Class:     com_indeed_imhotep_local_MultiRegroupInternals
 * Method:    nativeRemapDocsInTargetGroups
 * Signature: (Lcom/indeed/imhotep/local/GroupLookup;Lcom/indeed/imhotep/local/GroupLookup;[ILcom/indeed/flamdex/api/DocIdStream;[II)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_MultiRegroupInternals_nativeRemapDocsInTargetGroups
  (JNIEnv *java_env, jclass class, jobject doc_id_group, jobject new_lookup,
   jlong doc_list_address, jintArray remappings, jint placeholder_group)
{
  jlong doc_id_group_addr = getNativeShardDataPtr(java_env, doc_id_group);
  jlong new_lookup_addr   = getNativeShardDataPtr(java_env, new_lookup);

  jboolean made_copy        = 0;
	jint*    remappings_array = (*java_env)->GetPrimitiveArrayCritical(java_env, remappings, &made_copy);

  int status = remap_docs_in_target_groups((packed_table_t*) doc_id_group_addr,
                                           (packed_table_t*) new_lookup_addr,
                                           (uint8_t*) doc_list_address,
                                           remappings_array,
                                           placeholder_group);

	(*java_env)->ReleasePrimitiveArrayCritical(java_env, remappings, remappings_array, JNI_ABORT);

  if (status != 0) {
    jclass exClass = (*java_env)->FindClass(java_env, "java/lang/IllegalArgumentException");
    (*java_env)->ThrowNew(java_env, exClass,
                          "Regrouping on a multi-valued field doesn't work correctly so the "
                          "operation is rejected.");
  }
}
