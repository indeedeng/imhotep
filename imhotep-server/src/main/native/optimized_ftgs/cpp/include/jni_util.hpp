#ifndef JNI_UTIL_HPP
#define JNI_UTIL_HPP

#include <jni.h>

#include <stdexcept>
#include <string>
#include <vector>

namespace imhotep {

    typedef std::vector<std::string> strvec_t;

    template <typename java_type, typename value_type>
    value_type from_java(JNIEnv* env, java_type value)
    {
        return value_type(value);
    }

    template<> inline
    std::string from_java<jstring, std::string>(JNIEnv* env, jstring value)
    {
        jboolean    unused(false);
        const char* content(env->GetStringUTFChars(value, &unused));
        std::string result(content);
        env->ReleaseStringUTFChars(value, content);
        return result;
    }

    template<> inline
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

    inline void die_if(bool condition, std::string what) {
        if (condition) throw std::runtime_error(what);
    }

} //  namespace imhotep

#endif
