#include <jni.h>

#undef  JNIEXPORT
#define JNIEXPORT __attribute__((visibility("default")))

#include "jni/com_indeed_imhotep_local_ImhotepNativeLocalSession.h"

#include <iostream>             // !@# debugging
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include "binder.hpp"
#include "group_multi_remap_rule.hpp"
#include "imhotep_error.hpp"
#include "regroup_condition.hpp"

namespace imhotep {

    class RegroupConditionBinder : public Binder {
    public:
        RegroupConditionBinder(JNIEnv* env)
            : Binder(env, "com/indeed/imhotep/RegroupCondition")
            , _field(field_id_for("field", "Ljava/lang/String;"))
            , _int_type(field_id_for("intType", "Z"))
            , _int_term(field_id_for("intTerm", "J"))
            , _string_term(field_id_for("stringTerm", "Ljava/lang/String;"))
            , _inequality(field_id_for("inequality", "Z")) {
        }

        RegroupCondition operator()(jobject obj) {
            const std::string field(string_field(obj, _field));
            const jboolean    int_type(env()->GetBooleanField(obj, _int_type));
            const jboolean    inequality(env()->GetBooleanField(obj, _inequality));
            if (int_type) {
                const int64_t term(env()->GetLongField(obj, _int_term));
                return inequality ?
                    RegroupCondition(IntInequality(field, term)) :
                    RegroupCondition(IntEquality(field, term));
            }
            else {
                const std::string term(string_field(obj, _string_term));
                return inequality ?
                    RegroupCondition(StrInequality(field, term)) :
                    RegroupCondition(StrEquality(field, term));
            }
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
            jintArray     positives(object_field<jintArray>(obj, _positive));
            jobjectArray  conditions(object_field<jobjectArray>(obj, _conditions));
            const jsize   positive_length(env()->GetArrayLength(positives));
            const jsize   condition_length(env()->GetArrayLength(conditions));
            if (positive_length != condition_length) {
                std::ostringstream os;
                os << __FUNCTION__ << ":"
                   << " positive_length(" << positive_length << ")" << " !="
                   << " condition_length(" << condition_length << ")";
                throw imhotep_error(__FUNCTION__); // !@# improve message...
            }

            GroupMultiRemapRule::Rules rules;

            jint* positive_values(env()->GetIntArrayElements(positives, NULL));
            if (positive_values == NULL) {
                std::ostringstream os;
                os << __FUNCTION__ << ": could not retrieve 'positive' int array elements";
                throw imhotep_error(os.str());
            }

            try {
                for (jsize index(0); index < positive_length; ++index) {
                    const int32_t positive(positive_values[index]);
                    jobject       condition(env()->GetObjectArrayElement(conditions, index));
                    if (condition == NULL) {
                        std::ostringstream os;
                        os << __FUNCTION__ << ": could not retrieve 'condition' object array element";
                        throw imhotep_error(os.str());
                    }
                    rules.emplace_back(GroupMultiRemapRule::Rule(positive,
                                                                 _condition_binder(condition)));
                }
            }
            catch (...) {
                env()->ReleaseIntArrayElements(positives, positive_values, JNI_ABORT);
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
    // !@# TODO(johnf): Make binder instances static, so that we don't
    // pay the penalty for FindClass, etc. on each call.
    // http://www.ibm.com/developerworks/library/j-jni/

    static std::mutex bh; {
        std::lock_guard<std::mutex> guard(bh);
        std::cerr << "rules: " << rules << std::endl;
    }

    jlong result(0);
    try {
        GroupMultiRemapRuleBinder         binder(env);
        jsize                             num_rules(env->GetArrayLength(rules));
        std::vector<GroupMultiRemapRule>* rule_vector(new std::vector<GroupMultiRemapRule>());
        try {
            rule_vector->reserve(num_rules);
            for (jsize index(0); index < num_rules; ++index) {
                jobject rule(env->GetObjectArrayElement(rules, index));
                if (rule == NULL) throw imhotep_error("rule is null");
                rule_vector->emplace_back(binder(rule));
            }
            result = reinterpret_cast<jlong>(rule_vector);
        }
        catch (...) {
            delete rule_vector;
            throw;
        }
    }
    catch (const std::exception& ex) {
        jclass exClass = env->FindClass("java/lang/RuntimeException");
        env->ThrowNew(exClass, ex.what());
    }
    return result;
}

/*
 * Class:     com_indeed_imhotep_local_ImhotepNativeLocalSession
 * Method:    nativeReleaseRules
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_com_indeed_imhotep_local_ImhotepNativeLocalSession_nativeReleaseRules(JNIEnv* env,
                                                                           jclass  unusedClass,
                                                                           jlong   nativeRulesPtr)
{
    std::vector<GroupMultiRemapRule>* rules(reinterpret_cast<std::vector<GroupMultiRemapRule>*>(nativeRulesPtr));
    if (rules) delete rules;
}

/*
 * Class:     com_indeed_imhotep_local_ImhotepNativeLocalSession
 * Method:    nativeRegroup
 * Signature: (JJZ)I
 */
JNIEXPORT jint JNICALL
Java_com_indeed_imhotep_local_ImhotepNativeLocalSession_nativeRegroup(JNIEnv *env,
                                                                      jclass unusedClass,
                                                                      jlong nativeRulesPtr,
                                                                      jlong nativeShardDataPtr,
                                                                      jboolean errorOnCollisions)
{
    jint result = 0;
    std::vector<GroupMultiRemapRule>* rulesPtr =
        reinterpret_cast<std::vector<GroupMultiRemapRule>*>(nativeRulesPtr);
    try {
        if (!rulesPtr) {
            std::ostringstream message;
            message << __PRETTY_FUNCTION__ << " null 'rules' argument";
            throw imhotep_error(message.str());
        }
        if (!nativeShardDataPtr) {
            std::ostringstream message;
            message << __PRETTY_FUNCTION__ << " null 'shard' argument";
            throw imhotep_error(message.str());
        }
        std::cerr << __PRETTY_FUNCTION__ << " excelsior!" << std::endl;
    }
    catch (const std::exception& ex) {
        jclass exClass = env->FindClass("java/lang/RuntimeException");
        env->ThrowNew(exClass, ex.what());
    }
    return result;
}

