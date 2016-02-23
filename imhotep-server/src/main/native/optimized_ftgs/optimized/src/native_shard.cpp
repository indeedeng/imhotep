#include <jni.h>

#undef  JNIEXPORT
#define JNIEXPORT __attribute__((visibility("default")))

#include "jni/com_indeed_imhotep_local_NativeShard.h"

#include "jni_util.hpp"
#include "shard.hpp"

using namespace imhotep;

/*
 * Class:     com_indeed_imhotep_local_NativeShard
 * Method:    nativeGetShard
 * Signature: (Ljava/lang/String;[Ljava/lang/String;[Ljava/lang/String;J[Ljava/lang/String;[J)J
 */
JNIEXPORT jlong JNICALL
Java_com_indeed_imhotep_local_NativeShard_nativeGetShard(JNIEnv*      env,
                                                         jclass       unusedClass,
                                                         jstring      shardDir,
                                                         jobjectArray intFields,
                                                         jobjectArray strFields,
                                                         jlong        packedTablePtr,
                                                         jobjectArray mappedFiles,
                                                         jlongArray   mappedPtrs)
{
    const std::string shard_dir(from_java<jstring, std::string>(env(), shardDir));

    const strvec_t    int_fields(from_java_array<std::string>(env(), intFields));
    const strvec_t    str_fields(from_java_array<std::string>(env(), strFields));

    const Shard::packed_table_ptr packed_table_ptr(from_java<jlong,
                                                   Shard::packed_table_ptr>(packedTablePtr);

    return 0L;
}

/*
 * Class:     com_indeed_imhotep_local_NativeShard
 * Method:    nativeReleaseShard
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_com_indeed_imhotep_local_NativeShard_nativeReleaseShard(JNIEnv* env,
                                                             jclass  unusedClass,
                                                             jlong   nativeShardPtr)
{
    Shard* shardPtr(reinterpret_cast<Shard*>(nativeShardPtr));
    if (shardPtr != 0) {
        delete shardPtr;
    }
}

