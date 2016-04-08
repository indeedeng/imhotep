/** @file run_native_ftgs.cpp implements MTImhotepLocalMultiSession::nativeFTGS().

    A stylistic note: this module contains a deliberate mix of camel case and
    underscore-separated variable. The former are used to refer to Java entities
    and the latter c++ ones.
 */

#include <jni.h>

#undef  JNIEXPORT
#define JNIEXPORT               __attribute__((visibility("default")))

#include "run_native_ftgs.hpp"  // !@# rename

#include <algorithm>
#include <array>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "binder.hpp"
#include "executor_service.hpp"
#include "ftgs_runner.hpp"
#include "jni_util.hpp"
#include "log.hpp"
#include "shard.hpp"

using namespace imhotep;

class NativeFTGSRunnable : public Binder {
public:
    NativeFTGSRunnable(JNIEnv* env)
        : Binder(env, "com/indeed/imhotep/local/MTImhotepLocalMultiSession$NativeFTGSRunnable")
        , _shardPtrs(field_id_for("shardPtrs", "[J"))
        , _onlyBinaryMetrics(field_id_for("onlyBinaryMetrics", "Z"))
        , _intFields(field_id_for("intFields", "[Ljava/lang/String;"))
        , _stringFields(field_id_for("stringFields", "[Ljava/lang/String;"))
        , _splitsDir(field_id_for("splitsDir", "Ljava/lang/String;"))
        , _numGroups(field_id_for("numGroups", "I"))
        , _numStats(field_id_for("numStats", "I"))
        , _numSplits(field_id_for("numSplits", "I"))
        , _numWorkers(field_id_for("numWorkers", "I"))
        , _socketFDs(field_id_for("socketFDs", "[I"))
    { }

    void run(jobject obj) {
        jlongArray   shardPtrs(object_field<jlongArray>(obj, _shardPtrs));
        jboolean     onlyBinaryMetrics(env()->GetBooleanField(obj, _onlyBinaryMetrics));
        jobjectArray intFields(object_field<jobjectArray>(obj, _intFields));
        jobjectArray stringFields(object_field<jobjectArray>(obj, _stringFields));
        jstring      splitsDir(object_field<jstring>(obj, _splitsDir));
        jint         numGroups(env()->GetIntField(obj, _numGroups));
        jint         numStats(env()->GetIntField(obj, _numStats));
        jint         numSplits(env()->GetIntField(obj, _numSplits));
        jint         numWorkers(env()->GetIntField(obj, _numWorkers));
        jintArray    socketFDs(object_field<jintArray>(obj, _socketFDs));
        run(shardPtrs,onlyBinaryMetrics, intFields, stringFields, splitsDir,
            numGroups, numStats, numSplits, numWorkers, socketFDs);
    }

private:
    void run(jlongArray   shardPtrs,
             jboolean     onlyBinaryMetrics,
             jobjectArray intFields,
             jobjectArray strFields,
             jstring      splitsDir,
             jint         numGroups,
             jint         numStats,
             jint         numSplits,
             jint         numWorkers,
             jintArray    socketFDs) {

        const std::vector<long> shard_ptrs(from_java_array<long>(env(), shardPtrs));
        std::vector<Shard*> shards;
        for (size_t index(0); index < shard_ptrs.size(); ++index) {
            Shard* shard(reinterpret_cast<Shard*>(shard_ptrs[index]));
            shards.push_back(shard);
        }

        const strvec_t int_fields(from_java_array<std::string>(env(), intFields));
        const strvec_t str_fields(from_java_array<std::string>(env(), strFields));

        const std::string splits_dir(from_java<jstring, std::string>(env(), splitsDir));
        const size_t      num_splits(from_java<jint, size_t>(env(), numSplits));
        const size_t      num_workers(from_java<jint, size_t>(env(), numWorkers));

        ExecutorService executor;
        FTGSRunner runner(shards, int_fields, str_fields, splits_dir,
                          num_splits, num_workers, executor);

        const int        num_groups(from_java<jint, int>(env(), numGroups));
        const int        num_metrics(from_java<jint, int>(env(), numStats));
        std::vector<int> socket_fds(from_java_array<int>(env(), socketFDs));

        const bool only_binary_metrics(from_java<jboolean, bool>(env(), onlyBinaryMetrics));

        runner.run(num_groups, num_metrics, only_binary_metrics,
                   shards[0]->table(), socket_fds);
    }

    jfieldID _shardPtrs, _onlyBinaryMetrics, _intFields, _stringFields, _splitsDir,
        _numGroups, _numStats, _numSplits, _numWorkers, _socketFDs;
};

/*
 * Class:     com_indeed_imhotep_local_MTImhotepLocalMultiSession_NativeFTGSRunnable
 * Method:    run
 * Signature: ()V
 */
JNIEXPORT void JNICALL
Java_com_indeed_imhotep_local_MTImhotepLocalMultiSession_00024NativeFTGSRunnable_run
(JNIEnv* env, jobject obj) {

    try {
        // !@# Consider a static unique_ptr to NativeFTGSRunnable that we
        // initialize once. To do this, I believe we need to env->NewGlobalRef
        // the jclass for NativeFTGSRunnable lest it get unloaded from under
        // us. For now, this approach is simpler and one would hope not a
        // serious bottleneck.
        NativeFTGSRunnable runnable(env);
        runnable.run(obj);
    }
    catch (const std::exception& ex) {
        jclass exClass = env->FindClass("java/lang/RuntimeException");
        env->ThrowNew(exClass, ex.what());
    }

}

