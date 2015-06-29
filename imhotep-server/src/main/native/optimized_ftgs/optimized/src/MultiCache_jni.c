#include <jni.h>
#include "imhotep_native.h"
#include "local_session.h"

#undef  JNIEXPORT
#define JNIEXPORT               __attribute__((visibility("default")))

#include "com_indeed_imhotep_local_MultiCache.h"
#include "com_indeed_imhotep_local_MultiCache_MultiCacheGroupLookup.h"
#include "com_indeed_imhotep_local_MultiCache_MultiCacheIntValueLookup.h"

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
                                       jintArray sizes_bytes_array,
                                       jintArray vector_nums_array,
                                       jintArray offsets_in_vecs_array,
                                       jbyteArray original_order_arr,
                                       jint n_stats,
                                       jboolean only_binary_metrics)
{
    packed_table_t *table;
    jboolean unused;
    jlong *mins;
    jlong *maxes;
    jint *sizes;
    jint *vec_nums;
    jint *offests_in_vecs;
    jbyte *original_order;

    mins = (*env)->GetPrimitiveArrayCritical(env, mins_jarray, &unused);
    maxes = (*env)->GetPrimitiveArrayCritical(env, maxes_jarray, &unused);
    sizes = (*env)->GetPrimitiveArrayCritical(env, sizes_bytes_array, &unused);
    vec_nums = (*env)->GetPrimitiveArrayCritical(env, vector_nums_array, &unused);
    offests_in_vecs = (*env)->GetPrimitiveArrayCritical(env, offsets_in_vecs_array, &unused);
    original_order = (*env)->GetPrimitiveArrayCritical(env, original_order_arr, &unused);
    table = create_shard_multicache(n_docs, mins, maxes, sizes, vec_nums, offests_in_vecs,
                                    original_order, n_stats, only_binary_metrics);
    (*env)->ReleasePrimitiveArrayCritical(env, original_order_arr, original_order, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, offsets_in_vecs_array, offests_in_vecs, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, vector_nums_array, vec_nums, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, sizes_bytes_array, sizes, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, maxes_jarray, maxes, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, mins_jarray, mins, JNI_ABORT);

    return (jlong) table;
}

JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_MultiCache_nativeDestroyMultiCache
                                      (JNIEnv *env,
                                       jobject java_multicache,
                                       jlong table_pointer)
{
    packed_table_t *table = (packed_table_t *)table_pointer;

    destroy_shard_multicache(table);
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
    jboolean unused;
    jlong *values;

    values = (*env)->GetPrimitiveArrayCritical(env, values_jarray, &unused);
    packed_table_set_col_range(table, start, values, count, metric_id);
    (*env)->ReleasePrimitiveArrayCritical(env, values_jarray, values, JNI_ABORT);
}

/*
 * Class:     com_indeed_imhotep_local_MultiCache
 * Method:    nativeSetGroupsInRange
 * Signature: (JII[I)V
 */
JNIEXPORT void JNICALL Java_com_indeed_imhotep_local_MultiCache_nativeSetGroupsInRange
                                    (JNIEnv *env,
                                     jobject java_multicache_obj,
                                     jlong table_pointer,
                                     jint start,
                                     jint count,
                                     jintArray values_jarray)
{
    packed_table_t *table = (packed_table_t *)table_pointer;
    jboolean unused;
    jint *values;

    values = (*env)->GetPrimitiveArrayCritical(env, values_jarray, &unused);
    packed_table_set_group_range(table, start, count, values);
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
    jboolean unused_docIds;
    jboolean unused_values;
    jint *rows_ids;
    jlong *results;

    rows_ids = (*env)->GetPrimitiveArrayCritical(env, docIds_jarray, &unused_docIds);
    results = (*env)->GetPrimitiveArrayCritical(env, results_jarray, &unused_values);
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
    jboolean unused_docIds;
    jboolean unused_results;
    jint *rows_ids;
    jint *results;

    rows_ids = (*env)->GetPrimitiveArrayCritical(env, docIds_jarray, &unused_docIds);
    results = (*env)->GetPrimitiveArrayCritical(env, groups_jarray, &unused_results);
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
    jboolean unused_docIds;
    jboolean unused_groups;
    jint *rows_ids;
    jint *groups;

    rows_ids = (*env)->GetPrimitiveArrayCritical(env, docIds_jarray, &unused_docIds);
    groups = (*env)->GetPrimitiveArrayCritical(env, groups_jarray, &unused_groups);
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
    jboolean unused;
    jint *values;

    values = (*env)->GetPrimitiveArrayCritical(env, values_jarray, &unused);
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
    jboolean unused;
    jlong *bits;

    bits = (*env)->GetPrimitiveArrayCritical(env, bitset_jarray, &unused);
    packed_table_bit_set_regroup(table, bits, target_group, negativeGroup, positiveGroup);
    (*env)->ReleasePrimitiveArrayCritical(env, bitset_jarray, bits, JNI_ABORT);
}

/*
 * Class:     com_indeed_imhotep_local_MultiCache_MultiCacheGroupLookup
 * Method:    nativeRecalculateNumGroups
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_com_indeed_imhotep_local_MultiCache_00024MultiCacheGroupLookup_nativeRecalculateNumGroups
  (JNIEnv *env,
   jobject java_group_lookup_obj,
   jlong table_pointer)
{
    packed_table_t *table = (packed_table_t *)table_pointer;
    jint result = packed_table_get_num_groups(table);
    return result;
}
