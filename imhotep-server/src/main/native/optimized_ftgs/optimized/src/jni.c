#include <jni.h>
#include "imhotep_native.h"
#include "local_session.h"
#include "remote_output.h"

/*
 * Class:     com_indeed_imhotep_multicache_ftgs_NativeFTGSWorker
 * Method:    native_start_field
 * Signature: (JJ[BIZ)I
 */
JNIEXPORT jint JNICALL Java_com_indeed_imhotep_multicache_ftgs_NativeFTGSWorker_native_1start_1field
						(JNIEnv *java_env,
						jclass class,
						jlong worker_addr,
						jlong session_addr,
						jbyteArray field_name_bytes_arr,
						jint field_name_len,
						jboolean is_int_field_jboolean)
{
	struct worker_desc *worker;
	struct session_desc *session;
	jbyte *field_name;
	uint32_t is_int_field;
	jboolean madeCopy;
	int err;

	worker = (struct worker_desc *)worker_addr;
	session = (struct session_desc *)session_addr;
	is_int_field = is_int_field_jboolean;

	field_name = (*java_env)->GetPrimitiveArrayCritical(java_env, field_name_bytes_arr, &madeCopy);
    err = worker_start_field(worker, (char *)field_name, field_name_len,
                             (is_int_field) ? TERM_TYPE_INT : TERM_TYPE_STRING);
	(*java_env)->ReleasePrimitiveArrayCritical(java_env, field_name_bytes_arr, field_name, JNI_ABORT);

	if (err != 0) {
		/* Note: ThrowNew() copies the message handed to it, as one
		 would expect. I could not find mention of this behavior in
		 the JNI spec, but I verified this empirically. Therefore,
		 it's okay to hand it the stack-allocated string below. */
		jclass exClass = (*java_env)->FindClass(java_env, "java/lang/RuntimeException");
		char message[SIZE_OF_ERRSTR];
		snprintf(message, sizeof(message), "%s (%d) %s", __FUNCTION__,
		         worker->error.code, worker->error.str);
		(*java_env)->ThrowNew(java_env, exClass, message);
	}

	return (jint)err;
}

/*
 * Class:     com_indeed_imhotep_multicache_ftgs_NativeFTGSWorker
 * Method:    native_end_field
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_com_indeed_imhotep_multicache_ftgs_NativeFTGSWorker_native_1end_1field
						(JNIEnv *java_env,
						jclass class,
						jlong worker_addr,
						jlong session_addr)
{
	struct worker_desc *worker;
	struct session_desc *session;
	int err;

	worker = (struct worker_desc *)worker_addr;
	session = (struct session_desc *)session_addr;

    err = worker_end_field(worker);

	if (err != 0) {
		/* Note: ThrowNew() copies the message handed to it, as one
		 would expect. I could not find mention of this behavior in
		 the JNI spec, but I verified this empirically. Therefore,
		 it's okay to hand it the stack-allocated string below. */
		jclass exClass = (*java_env)->FindClass(java_env, "java/lang/RuntimeException");
		char message[SIZE_OF_ERRSTR];
		snprintf(message, sizeof(message), "%s (%d) %s", __FUNCTION__,
		         worker->error.code, worker->error.str);
		(*java_env)->ThrowNew(java_env, exClass, message);
	}

	return (jint)err;
}

JNIEXPORT jlong JNICALL Java_com_indeed_imhotep_multicache_ftgs_NativeFTGSWorker_native_1init
  						(JNIEnv *java_env,
  						jclass class,
  						jint id,
  						jint n_groups,
  						jint n_metrics,
  						jintArray socket_fds,
  						jint len)
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
 * Class:     com_indeed_imhotep_multicache_ftgs_NativeFTGSWorker
 * Method:    native_session_create
 * Signature: (J[JIII)J
 */
JNIEXPORT jlong JNICALL Java_com_indeed_imhotep_multicache_ftgs_NativeFTGSWorker_native_1session_1create
						(JNIEnv *env,
						jclass class,
						jlong worker_addr,
						jlongArray shard_ptr_arr,
						jint n_shards,
						jint n_groups,
						jint n_metrics)
{
	struct session_desc *session;
	packed_table_t **shard_ptrs;
	jboolean madeCopy;

	session = calloc(1, sizeof(struct session_desc));
	shard_ptrs = (*env)->GetPrimitiveArrayCritical(env, shard_ptr_arr, &madeCopy);
	session_init(session, n_groups, n_metrics, shard_ptrs, n_shards);
	(*env)->ReleasePrimitiveArrayCritical(env, shard_ptr_arr, shard_ptrs, JNI_ABORT);

	return (jlong)session;
}

/*
 * Class:     com_indeed_imhotep_multicache_ftgs_NativeFTGSWorker
 * Method:    native_run_int_tgs_pass
 * Signature: (JJJ[J[III)I
 */
JNIEXPORT jint JNICALL Java_com_indeed_imhotep_multicache_ftgs_NativeFTGSWorker_native_1run_1int_1tgs_1pass
						(JNIEnv *java_env,
						jclass class,
						jlong worker_addr,
						jlong session_addr,
						jlong int_term,
						jlongArray slice_offsets_arr,
						jintArray docs_per_slice_arr,
						jint num_shards,
						jint socket_num)
{
	struct worker_desc *worker;
	struct session_desc *session;
	jlong *slice_offsets;
	jint *docs_per_slice;
	jboolean madeCopy;

	worker = (struct worker_desc *)worker_addr;
	session = (struct session_desc *)session_addr;

	slice_offsets = (*java_env)->GetPrimitiveArrayCritical(java_env, slice_offsets_arr, &madeCopy);
	docs_per_slice = (*java_env)->GetPrimitiveArrayCritical(java_env, docs_per_slice_arr, &madeCopy);

	int err = run_tgs_pass(worker,
							session,
							TERM_TYPE_INT,
							int_term,
							NULL,
							0,
							slice_offsets,
							docs_per_slice,
							num_shards,
							socket_num);

	(*java_env)->ReleasePrimitiveArrayCritical(java_env, docs_per_slice_arr, docs_per_slice, JNI_ABORT);
	(*java_env)->ReleasePrimitiveArrayCritical(java_env, slice_offsets_arr, slice_offsets, JNI_ABORT);

	if (err != 0) {
		/* Note: ThrowNew() copies the message handed to it, as one
		 would expect. I could not find mention of this behavior in
		 the JNI spec, but I verified this empirically. Therefore,
		 it's okay to hand it the stack-allocated string below. */
		jclass exClass = (*java_env)->FindClass(java_env, "java/lang/RuntimeException");
		char message[SIZE_OF_ERRSTR];
		snprintf(message, sizeof(message), "%s (%d) %s", __FUNCTION__,
		         worker->error.code, worker->error.str);
		(*java_env)->ThrowNew(java_env, exClass, message);
	}

	return (jint)err;
}

/*
 * Class:     com_indeed_imhotep_multicache_ftgs_NativeFTGSWorker
 * Method:    native_run_string_tgs_pass
 * Signature: (JJ[BI[J[III)I
 */
JNIEXPORT jint JNICALL Java_com_indeed_imhotep_multicache_ftgs_NativeFTGSWorker_native_1run_1string_1tgs_1pass
						(JNIEnv *java_env,
						jclass class,
						jlong worker_addr,
						jlong session_addr,
						jbyteArray string_term_bytes_arr,
						jint string_term_len,
						jlongArray slice_offsets_arr,
						jintArray docs_per_slice_arr,
						jint num_shards,
						jint socket_num)
{
	struct worker_desc *worker;
	struct session_desc *session;
	jlong *slice_offsets;
	jint *docs_per_slice;
	jbyte *string_term;
	jboolean madeCopy;

	worker = (struct worker_desc *)worker_addr;
	session = (struct session_desc *)session_addr;

	slice_offsets = (*java_env)->GetPrimitiveArrayCritical(java_env, slice_offsets_arr, &madeCopy);
	docs_per_slice = (*java_env)->GetPrimitiveArrayCritical(java_env, docs_per_slice_arr, &madeCopy);
	string_term = (*java_env)->GetPrimitiveArrayCritical(java_env, string_term_bytes_arr, &madeCopy);

	int err = run_tgs_pass(worker,
							session,
							TERM_TYPE_STRING,
							-1,
							(char *)string_term,
							string_term_len,
							slice_offsets,
							docs_per_slice,
							num_shards,
							socket_num);

	(*java_env)->ReleasePrimitiveArrayCritical(java_env, string_term_bytes_arr, string_term, JNI_ABORT);
	(*java_env)->ReleasePrimitiveArrayCritical(java_env, docs_per_slice_arr, docs_per_slice, JNI_ABORT);
	(*java_env)->ReleasePrimitiveArrayCritical(java_env, slice_offsets_arr, slice_offsets, JNI_ABORT);

	if (err != 0) {
		/* Note: ThrowNew() copies the message handed to it, as one
		 would expect. I could not find mention of this behavior in
		 the JNI spec, but I verified this empirically. Therefore,
		 it's okay to hand it the stack-allocated string below. */
		jclass exClass = (*java_env)->FindClass(java_env, "java/lang/RuntimeException");
		char message[SIZE_OF_ERRSTR];
		snprintf(message, sizeof(message), "%s (%d) %s", __FUNCTION__,
		         worker->error.code, worker->error.str);
		(*java_env)->ThrowNew(java_env, exClass, message);
	}

	return (jint)err;
}

/*
 * Class:     com_indeed_imhotep_multicache_ftgs_NativeFTGSWorker
 * Method:    native_session_destroy
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_multicache_ftgs_NativeFTGSWorker_native_1session_1destroy
//  (JNIEnv *, jclass, jlong, jlong);
						(JNIEnv *java_env,
						jclass class,
						jlong worker_addr,
						jlong session_addr)
{
	struct worker_desc *worker;
	struct session_desc *session;

	worker = (struct worker_desc *)worker_addr;
	session = (struct session_desc *)session_addr;
	session_destroy(session);
}

/*
 * Class:     com_indeed_imhotep_multicache_ftgs_NativeFTGSWorker
 * Method:    native_worker_destroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_multicache_ftgs_NativeFTGSWorker_native_1worker_1destroy
//  (JNIEnv *, jclass, jlong);
						(JNIEnv *java_env,
						jclass class,
						jlong worker_addr)
{
	struct worker_desc *worker;

	worker = (struct worker_desc *)worker_addr;
	worker_destroy(worker);
}


/*
 * Class:     com_indeed_imhotep_local_MultiRegroupInternals
 * Method:    nativeRemapDocsInTargetGroups
 * Signature: (J[IJI[II)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_MultiRegroupInternals_nativeRemapDocsInTargetGroups
(JNIEnv *java_env, jclass clazz,
 long native_shard_data_ptr, jintArray results,
 jlong doc_list_address, jint n_docs,
 jintArray remappings,
 jint placeholder_group)
{
    jboolean unused           = 0;
    jint*    results_array    = (*java_env)->GetPrimitiveArrayCritical(java_env, results, &unused);
    jint*    remappings_array = (*java_env)->GetPrimitiveArrayCritical(java_env, remappings, &unused);

    int status = remap_docs_in_target_groups((packed_table_t*) native_shard_data_ptr,
                                             results_array,
                                             (uint8_t*) doc_list_address, n_docs,
                                             remappings_array,
                                             placeholder_group);

    (*java_env)->ReleasePrimitiveArrayCritical(java_env, remappings, remappings_array, JNI_ABORT);
    (*java_env)->ReleasePrimitiveArrayCritical(java_env, results,    results_array,    0);

    if (status != 0) {
        jclass exClass = (*java_env)->FindClass(java_env, "java/lang/IllegalArgumentException");
        (*java_env)->ThrowNew(java_env, exClass,
                              "Regrouping on a multi-valued field doesn't work correctly so the "
                              "operation is rejected.");
    }
}
