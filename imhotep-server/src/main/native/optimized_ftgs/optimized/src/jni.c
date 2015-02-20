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

	fds = GetPrimitiveArrayCritical(java_env, socket_fds, &madeCopy);
	worker = calloc(sizeof(struct worker_desc), 1);
	worker_init(worker,id, n_groups, n_metrics, fds, len);
	ReleasePrimitiveArrayCritical(java_env, socket_fds, fds, JNI_ABORT);

	return (jlong)worker;
}



/*
 * Class:     com_indeed_imhotep_local_NativeFTGSWorker
 * Method:    native_session_create
 * Signature: (III)J
 */
JNIEXPORT jlong JNICALL Java_com_indeed_imhotep_local_NativeFTGSWorker_native_1session_1create
  (JNIEnv *java_env, jclass class, jint n_groups, jint n_metrics, jint n_shards)
{
	struct session_desc *session;

	session = calloc(sizeof(struct session_desc), 1);
	session_init(session, n_groups, n_metrics, n_shards);

	return (jlong)session;
}
