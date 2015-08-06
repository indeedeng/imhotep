#include "com_indeed_imhotep_local_ImhotepNativeLocalSession.h"

#include <jni.h>

#undef  JNIEXPORT
#define JNIEXPORT __attribute__((visibility("default")))

#include "com_indeed_imhotep_local_ImhotepNativeLocalSession.h"

#include <sstream>
#include <string>

#include "binder.hpp"
#include "group_multi_remap_rule.hpp"
#include "imhotep_error.hpp"
#include "regroup_condition.hpp"

namespace imhotep {

    class RegroupConditionBinder : public Binder {
    public:
        RegroupConditionBinder(JNIEnv* env)
            : Binder(env, "com/indeed/imhotep/RegroupCondition")
            , _field(field_id_for("field", "Ljava/lang/String"))
            , _int_type(field_id_for("intType", "Z"))
            , _int_term(field_id_for("intTerm", "J"))
            , _string_term(field_id_for("stringTerm", "Ljava/lang/String"))
            , _inequality(field_id_for("inequality", "Z"))
        { }

        RegroupCondition operator()(jobject obj) {
            // !@# Consider special cases for int_type == [true|false]
            return RegroupCondition(string_field(obj, _field),
                                    env()->GetBooleanField(obj, _int_type),
                                    env()->GetLongField(obj, _int_term),
                                    string_field(obj, _string_term),
                                    env()->GetBooleanField(obj, _inequality));
        }

    private:
        jfieldID _field, _int_type, _int_term, _string_term, _inequality;
    };

    class GroupMultiRemapRuleBinder : public Binder {
    public:
        GroupMultiRemapRuleBinder(JNIEnv* env)
            : Binder(env, "com/indeed/imhotep/GroupMultiRemapRule")
            , _condition_binder(env)
            , _target(field_id_for("targetGroup", "I"))
            , _negative(field_id_for("negativeGroup", "I"))
            , _positive(field_id_for("positiveGroups", "[I"))
            , _conditions(field_id_for("conditions", "[Lcom/indeed/imhotep/RegroupCondition;"))
        { }

        GroupMultiRemapRule operator()(jobject obj) {
            const int32_t target(env()->GetIntField(obj, _target));
            const int32_t negative(env()->GetIntField(obj, _negative));
            jobject positive(env()->GetObjectField(obj, _positive));
            jobject conditions(env()->GetObjectField(obj, _conditions));
            if (positive == NULL || conditions == NULL) {
                throw imhotep_error(__FUNCTION__);
            }

            jintArray positive_array(reinterpret_cast<jintArray>(positive));
            jsize positive_length(env()->GetArrayLength(positive_array));

            jobjectArray condition_array(reinterpret_cast<jobjectArray>(conditions));
            jsize condition_length(env()->GetArrayLength(condition_array));
            if (positive_length != condition_length) {
                throw imhotep_error(__FUNCTION__); // !@# improve message...
            }

            GroupMultiRemapRule::Rules rules;

            jint* positives(env()->GetIntArrayElements(positive_array, NULL));
            if (positives == NULL) throw imhotep_error(__FUNCTION__); // !@# improve message
            try {
                for (jsize index(0); index < positive_length; ++index) {
                    const int32_t positive(positives[index]);
                    const jobject condition(env()->GetObjectArrayElement(condition_array, index));
                    rules.emplace_back(GroupMultiRemapRule::Rule(positive, _condition_binder(condition)));
                }
            }
            catch (...) {
                env()->ReleaseIntArrayElements(positive_array, positives, JNI_ABORT);
                throw;
            }
            return GroupMultiRemapRule(target, negative, rules);
        }

    private:
        RegroupConditionBinder _condition_binder;

        jfieldID _target, _negative, _positive, _conditions;
    };

} //  namespace imhotep

using namespace imhotep;

/*
 * Class:     com_indeed_imhotep_local_ImhotepNativeLocalSession
 * Method:    nativeGetRules
 * Signature: ([Lcom/indeed/imhotep/GroupMultiRemapRule;)J
 */
JNIEXPORT jlong JNICALL
Java_com_indeed_imhotep_local_ImhotepNativeLocalSession_nativeGetRules(JNIEnv* env,
                                                                       jclass unusedClass,
                                                                       jobjectArray rules)
{
    return 0;
}

/*
 * Class:     com_indeed_imhotep_local_ImhotepNativeLocalSession
 * Method:    nativeReleaseRules
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_com_indeed_imhotep_local_ImhotepNativeLocalSession_nativeReleaseRules(JNIEnv* env,
                                                                           jclass unusedClass,
                                                                           jlong nativeRulesPtr)
{
}
