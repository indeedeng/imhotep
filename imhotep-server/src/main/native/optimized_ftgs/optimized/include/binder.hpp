#pragma once
#include <jni.h>

#include <sstream>
#include <string>

#include "imhotep_error.hpp"

namespace imhotep {

    class Binder {
    public:
        Binder(JNIEnv* env, jclass clazz)
            : _env(env)
            , _class(clazz)
        { }

        Binder(JNIEnv* env, const std::string& class_name)
            : _env(env)
            , _class(class_for(class_name))
        { }

        JNIEnv*   env() { return _env;   }
        jclass  clazz() { return _class; }

        jclass class_for(const std::string& class_name) {
            const jclass result(env()->FindClass(class_name.c_str()));
            if (result == NULL) {
                std::ostringstream os;
                os << __FUNCTION__ << ": FindClass() failed for class_name: " << class_name;
                throw imhotep_error(os.str());
            }
            return result;
        }

        jfieldID field_id_for(const std::string& name, const std::string& sig) {
            const jfieldID result(env()->GetFieldID(clazz(), name.c_str(), sig.c_str()));
            if (result == NULL) {
                std::ostringstream os;
                os << __FUNCTION__ << ": GetFieldID() failed for"
                   << " name: " << name
                   << " sig: "  << sig;
                throw imhotep_error(os.str());
            }
            return result;
        }

        jmethodID method_id_for(const std::string& name, const std::string& sig) {
            const jmethodID result(env()->GetMethodID(clazz(), name.c_str(), sig.c_str()));
            if (result == NULL) {
                std::ostringstream os;
                os << __FUNCTION__ << ": GetMethodID() failed for"
                   << " name: " << name
                   << " sig: "  << sig;
                throw imhotep_error(os.str());
            }
            return result;
        }

        std::string to_string(jstring value) {
            jsize       length(env()->GetStringUTFLength(value));
            const char* chars(env()->GetStringUTFChars(value, NULL));
            if (!chars) {
                return std::string();
            }
            else {
                const std::string result(chars, length);
                env()->ReleaseStringUTFChars(value, chars);
                return result;
            }
        }

        template <typename ResultType>
        ResultType object_field(jobject obj, jfieldID field) {
            jobject result(env()->GetObjectField(obj, field));
            if (result == NULL) {
                std::ostringstream os;
                os << __FUNCTION__ << ": GetObjectField() failed for"
                   << " obj: " << obj
                   << " field: " << field;
                throw imhotep_error(os.str());
            }
            return reinterpret_cast<ResultType>(result);
        }

        std::string string_field(jobject obj, jfieldID field) {
            jstring value(object_field<jstring>(obj, field));
            return to_string(value);
        }
        
    protected:
        JNIEnv* _env;
        jclass  _class;
    };

} // namespace imhotep
