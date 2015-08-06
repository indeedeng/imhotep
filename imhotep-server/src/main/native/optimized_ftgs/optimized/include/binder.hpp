#include <jni.h>

#include <sstream>
#include <string>

#include "imhotep_error.hpp"

namespace imhotep {

    class Binder {
    public:
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
                   << " sig: " << sig;
                throw imhotep_error(os.str());
            }
            return result;
        }

        std::string string_field(jobject obj, jfieldID field) {
            jstring value(reinterpret_cast<jstring>(env()->GetObjectField(obj, field)));
            if (value == NULL) throw imhotep_error(__FUNCTION__);

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

    private:
        JNIEnv* _env;
        jclass  _class;
    };

} // namespace imhotep
