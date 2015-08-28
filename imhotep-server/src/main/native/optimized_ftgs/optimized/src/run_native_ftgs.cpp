/** @file run_native_ftgs.cpp implements MTImhotepLocalMultiSession::nativeFTGS().

    A stylistic note: this module contains a deliberate mix of camel case and
    underscore-separated variable. The former are used to refer to Java entities
    and the latter c++ ones.
 */

#include <jni.h>

#undef  JNIEXPORT
#define JNIEXPORT               __attribute__((visibility("default")))

#include "run_native_ftgs.hpp"

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
#include "log.hpp"
#include "shard.hpp"

namespace imhotep {

    typedef std::vector<std::string> strvec_t;
    typedef Shard::packed_table_ptr  table_ptr;

    template <typename java_type, typename value_type>
    value_type from_java(JNIEnv* env, java_type value)
    {
        return value_type(value);
    }

    template<>
    std::string from_java<jstring, std::string>(JNIEnv* env, jstring value)
    {
        jboolean    unused(false);
        const char* content(env->GetStringUTFChars(value, &unused));
        std::string result(content);
        env->ReleaseStringUTFChars(value, content);
        return result;
    }

    template<>
    std::string from_java<jobject, std::string>(JNIEnv* env, jobject value)
    {
        return from_java<jstring, std::string>(env, static_cast<jstring>(value));
    }

    template <typename value_type>
    std::vector<value_type> from_java_array(JNIEnv* env, jobjectArray values)
    {
        jsize                   valuesSize(env->GetArrayLength(values));
        std::vector<value_type> result(valuesSize);
        for (jsize index(0); index < valuesSize; ++index) {
            result[index] =
                from_java<jobject, value_type>(env, env->GetObjectArrayElement(values, index));
        }
        return result;
    }

    template <typename value_type>
    std::vector<value_type> from_java_array(JNIEnv* env, jlongArray values)
    {
        jsize                   valuesSize(env->GetArrayLength(values));
        std::vector<value_type> result(valuesSize);
        jboolean                unused(false);
        jlong*                  elements(env->GetLongArrayElements(values, &unused));
        for (jsize index(0); index < valuesSize; ++index) {
            result[index] = value_type(elements[index]);
        }
        env->ReleaseLongArrayElements(values, elements, JNI_ABORT);
        return result;
    }

    template <typename value_type>
    std::vector<value_type> from_java_array(JNIEnv* env, jintArray values)
    {
        jsize                   valuesSize(env->GetArrayLength(values));
        std::vector<value_type> result(valuesSize);
        jboolean                unused(false);
        jint*                   elements(env->GetIntArrayElements(values, &unused));
        for (jsize index(0); index < valuesSize; ++index) {
            result[index] = value_type(elements[index]);
        }
        env->ReleaseIntArrayElements(values, elements, JNI_ABORT);
        return result;
    }

    void die_if(bool condition, std::string what) {
        if (condition) throw std::runtime_error(what);
    }

} //  namespace imhotep

using namespace imhotep;

class NativeFTGSRunnable : public Binder {
public:
    NativeFTGSRunnable(JNIEnv* env)
        : Binder(env, "com/indeed/imhotep/local/MTImhotepLocalMultiSession$NativeFTGSRunnable")
        , _shardDirs(field_id_for("shardDirs", "[Ljava/lang/String;"))
        , _packedTablePtrs(field_id_for("packedTablePtrs", "[J"))
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
        jobjectArray shardDirs(object_field<jobjectArray>(obj, _shardDirs));
        jlongArray   packedTablePtrs(object_field<jlongArray>(obj, _packedTablePtrs));
        jboolean     onlyBinaryMetrics(env()->GetBooleanField(obj, _onlyBinaryMetrics));
        jobjectArray intFields(object_field<jobjectArray>(obj, _intFields));
        jobjectArray stringFields(object_field<jobjectArray>(obj, _stringFields));
        jstring      splitsDir(object_field<jstring>(obj, _splitsDir));
        jint         numGroups(env()->GetIntField(obj, _numGroups));
        jint         numStats(env()->GetIntField(obj, _numStats));
        jint         numSplits(env()->GetIntField(obj, _numSplits));
        jint         numWorkers(env()->GetIntField(obj, _numWorkers));
        jintArray    socketFDs(object_field<jintArray>(obj, _socketFDs));
        run(shardDirs, packedTablePtrs, onlyBinaryMetrics, intFields, stringFields,
            splitsDir, numGroups, numStats, numSplits, numWorkers, socketFDs);
    }

private:
    void run(jobjectArray shardDirs,
             jlongArray   packedTablePtrs,
             jboolean     onlyBinaryMetrics,
             jobjectArray intFields,
             jobjectArray strFields,
             jstring      splitsDir,
             jint         numGroups,
             jint         numStats,
             jint         numSplits,
             jint         numWorkers,
             jintArray    socketFDs) {

        const strvec_t shard_dirs(from_java_array<std::string>(env(), shardDirs));
        const strvec_t int_fields(from_java_array<std::string>(env(), intFields));
        const strvec_t str_fields(from_java_array<std::string>(env(), strFields));

        const std::vector<table_ptr> table_ptrs
            (from_java_array<table_ptr>(env(), packedTablePtrs));
        die_if(shard_dirs.size() != table_ptrs.size(),
               "shard_dirs.size() != table_ptrs.size()");

        std::vector<Shard> shards;
        for (size_t index(0); index < shard_dirs.size(); ++index) {
            shards.push_back(Shard(shard_dirs[index],
                                   int_fields, str_fields,
                                   table_ptrs[index]));
        }

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
                   shards[0].table(), socket_fds);
    }

    jfieldID _shardDirs, _packedTablePtrs, _onlyBinaryMetrics,
        _intFields, _stringFields, _splitsDir,
        _numGroups, _numStats, _numSplits, _numWorkers,
        _socketFDs;
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
        NativeFTGSRunnable runnable(env);
        runnable.run(obj);
    }
    catch (const std::exception& ex) {
        jclass exClass = env->FindClass("java/lang/RuntimeException");
        env->ThrowNew(exClass, ex.what());
    }

}

