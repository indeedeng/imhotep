#include <jni.h>
#include "imhotep_native.h"
#include "local_session.h"

#undef com_indeed_imhotep_local_MultiCache_BLOCK_COPY_SIZE
#define com_indeed_imhotep_local_MultiCache_BLOCK_COPY_SIZE 8192L
/*
 * Class:     com_indeed_imhotep_local_MultiCache
 * Method:    nativeBuildMultiCache
 * Signature: (JI[I[II)J
 */
JNIEXPORT jlong JNICALL Java_com_indeed_imhotep_local_MultiCache_nativeBuildMultiCache
                                      (JNIEnv *env,
                                       jobject java_multicache,
                                       jint n_docs,
                                       jlongArray mins_jarray,
                                       jlongArray maxes_jarray,
                                       jint n_stats)
{
    packed_table_t *table;
    jboolean madeCopy_mins;
    jboolean madeCopy_maxes;
    jlong *mins;
    jlong *maxes;

    mins = (*env)->GetPrimitiveArrayCritical(env, mins_jarray, &madeCopy_mins);
    maxes = (*env)->GetPrimitiveArrayCritical(env, maxes_jarray, &madeCopy_maxes);
    table = create_shard_multicache(n_docs, mins, maxes, n_stats);
    (*env)->ReleasePrimitiveArrayCritical(env, maxes_jarray, maxes, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, mins_jarray, mins, JNI_ABORT);

    return (jlong) table;
}

/*
 * Class:     com_indeed_imhotep_local_MultiCache
 * Method:    nativePackMetricDataInRange
 * Signature: (JIII[J)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_MultiCache_nativePackMetricDataInRange
                                      (JNIEnv *env,
                                       jclass java_multicache,
                                       jlong table_pointer,
                                       jint metric_id,
                                       jint start,
                                       jint count,
                                       jlongArray values_jarray)
{
    packed_table_t *table = (packed_table_t *)table_pointer;
    jboolean madeCopy;
    jlong *values;

    values = (*env)->GetPrimitiveArrayCritical(env, values_jarray, &madeCopy);
    packed_table_set_col_range(table, start, values, count, metric_id);
    (*env)->ReleasePrimitiveArrayCritical(env, values_jarray, values, JNI_ABORT);
}

/*
 * Class:     com_indeed_imhotep_local_MultiCache_MultiCacheIntValueLookup
 * Method:    nativeMetricLookup
 * Signature: (JI[I[JI)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_MultiCache_00024MultiCacheIntValueLookup_nativeMetricLookup
                                     (JNIEnv *env,
                                      jobject java_int_value_lookup_obj,
                                      jlong table_pointer,
                                      jint idx,
                                      jintArray docIds_jarray,
                                      jlongArray results_jarray,
                                      jint count)
{
    packed_table_t *table = (packed_table_t *)table_pointer;
    jboolean madeCopy_docIds;
    jboolean madeCopy_values;
    jint *rows_ids;
    jlong *results;

    rows_ids = (*env)->GetPrimitiveArrayCritical(env, docIds_jarray, &madeCopy_docIds);
    results = (*env)->GetPrimitiveArrayCritical(env, results_jarray, &madeCopy_values);
    packed_table_batch_col_lookup(table, rows_ids, count, results, idx);
    (*env)->ReleasePrimitiveArrayCritical(env, results_jarray, results, 0);
    (*env)->ReleasePrimitiveArrayCritical(env, docIds_jarray, rows_ids, JNI_ABORT);
}

/*
 * Class:     com_indeed_imhotep_local_MultiCache_MultiCacheGroupLookup
 * Method:    nativeFillGroupsBuffer
 * Signature: (J[I[II)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_MultiCache_00024MultiCacheGroupLookup_nativeFillGroupsBuffer
                                     (JNIEnv *env,
                                      jobject java_group_lookup,
                                      jlong table_pointer,
                                      jintArray docIds_jarray,
                                      jintArray groups_jarray,
                                      jint count)
{
    packed_table_t *table = (packed_table_t *)table_pointer;
    jboolean madeCopy_docIds;
    jboolean madeCopy_results;
    jint *rows_ids;
    jint *results;

    rows_ids = (*env)->GetPrimitiveArrayCritical(env, docIds_jarray, &madeCopy_docIds);
    results = (*env)->GetPrimitiveArrayCritical(env, groups_jarray, &madeCopy_results);
    packed_table_batch_group_lookup(table, rows_ids, count, results);
    (*env)->ReleasePrimitiveArrayCritical(env, groups_jarray, results, 0);
    (*env)->ReleasePrimitiveArrayCritical(env, docIds_jarray, rows_ids, JNI_ABORT);
}

/*
 * Class:     com_indeed_imhotep_local_MultiCache_MultiCacheGroupLookup
 * Method:    nativeUpdateGroups
 * Signature: (J[I[II)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_MultiCache_00024MultiCacheGroupLookup_nativeUpdateGroups
                                     (JNIEnv *env,
                                      jobject java_group_lookup,
                                      jlong table_pointer,
                                      jintArray docIds_jarray,
                                      jintArray groups_jarray,
                                      jint count)
{
    packed_table_t *table = (packed_table_t *)table_pointer;
    jboolean madeCopy_docIds;
    jboolean madeCopy_groups;
    jint *rows_ids;
    jint *groups;

    rows_ids = (*env)->GetPrimitiveArrayCritical(env, docIds_jarray, &madeCopy_docIds);
    groups = (*env)->GetPrimitiveArrayCritical(env, groups_jarray, &madeCopy_groups);
    packed_table_batch_set_group(table, rows_ids, count, groups);
    (*env)->ReleasePrimitiveArrayCritical(env, groups_jarray, groups, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, docIds_jarray, rows_ids, JNI_ABORT);
}

/*
 * Class:     com_indeed_imhotep_local_MultiCache_MultiCacheGroupLookup
 * Method:    nativeUpdateGroupsSequential
 * Signature: (JII[I)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_MultiCache_00024MultiCacheGroupLookup_nativeUpdateGroupsSequential
                                        (JNIEnv *env,
                                         jobject java_group_lookup_obj,
                                         jlong table_pointer,
                                         jint start,
                                         jint count,
                                         jintArray values_jarray)
{
    packed_table_t *table = (packed_table_t *)table_pointer;
    jboolean madeCopy;
    jint *values;

    values = (*env)->GetPrimitiveArrayCritical(env, values_jarray, &madeCopy);
    packed_table_set_group_range(table, start, count, values);
    (*env)->ReleasePrimitiveArrayCritical(env, values_jarray, values, JNI_ABORT);
}

/*
 * Class:     com_indeed_imhotep_local_MultiCache_MultiCacheGroupLookup
 * Method:    nativeGetGroup
 * Signature: (JI)I
 */
JNIEXPORT jint JNICALL Java_com_indeed_imhotep_local_MultiCache_00024MultiCacheGroupLookup_nativeGetGroup
                                        (JNIEnv *env,
                                         jobject java_group_lookup_obj,
                                         jlong table_pointer,
                                         jint group)
{
    packed_table_t *table = (packed_table_t *)table_pointer;

    return (jint) packed_table_get_group(table, group);
}

/*
 * Class:     com_indeed_imhotep_local_MultiCache_MultiCacheGroupLookup
 * Method:    nativeSetGroupForDoc
 * Signature: (JII)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_MultiCache_00024MultiCacheGroupLookup_nativeSetGroupForDoc
                                        (JNIEnv *env,
                                         jobject java_group_lookup_obj,
                                         jlong table_pointer,
                                         jint doc_id,
                                         jint group)
{
    packed_table_t *table = (packed_table_t *)table_pointer;

    packed_table_set_group(table, doc_id, group);
}

/*
 * Class:     com_indeed_imhotep_local_MultiCache_MultiCacheGroupLookup
 * Method:    nativeSetAllGroups
 * Signature: (JI)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_MultiCache_00024MultiCacheGroupLookup_nativeSetAllGroups
                                        (JNIEnv *env,
                                         jobject java_group_lookup_obj,
                                         jlong table_pointer,
                                         jint group)
{
    packed_table_t *table = (packed_table_t *)table_pointer;

    packed_table_set_all_groups(table, group);
}

/*
 * Class:     com_indeed_imhotep_local_MultiCache_MultiCacheGroupLookup
 * Method:    nativeBitSetRegroup
 * Signature: (J[JIII)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_MultiCache_00024MultiCacheGroupLookup_nativeBitSetRegroup
                                        (JNIEnv *env,
                                         jobject java_group_lookup_obj,
                                         jlong table_pointer,
                                         jlongArray bitset_jarray,
                                         jint target_group,
                                         jint negativeGroup,
                                         jint positiveGroup)
{
    packed_table_t *table = (packed_table_t *)table_pointer;
    jboolean madeCopy;
    jlong *bits;

    bits = (*env)->GetPrimitiveArrayCritical(env, bitset_jarray, &madeCopy);
    packed_table_bit_set_regroup(table, bits, target_group, negativeGroup, positiveGroup);
    (*env)->ReleasePrimitiveArrayCritical(env, bitset_jarray, bits, JNI_ABORT);
}
