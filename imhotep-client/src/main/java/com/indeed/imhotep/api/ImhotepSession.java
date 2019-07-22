/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.imhotep.api;

import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.TermCount;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.Operator;
import com.indeed.imhotep.protobuf.StatsSortOrder;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@NotThreadSafe
public interface ImhotepSession
    extends Closeable, Instrumentation.Provider {

    String DEFAULT_GROUPS = ImhotepRequest.getDefaultInstance().getInputGroups();

    static List<String> stackStat(final int stat) {
        return Collections.singletonList("global_stack " + stat);
    }

    String getSessionId();

    /**
     * get the sum of the docFreq of all terms in all of the given fields
     * @param intFields int fields to iterate over
     * @param stringFields string fields to iterate over
     * @return the sum of all terms' docFreq
     */
    long getTotalDocFreq(String[] intFields, String[] stringFields);

    /**
     * get the current total of a given metric for each group
     * Trailing groups with 0 values can cause the returned array to be shorter than the total number of groups.
     * @param stat the metric
     * @return an array with the metric values, indexed by group
     */
    default long[] getGroupStats(final List<String> stat) throws ImhotepOutOfMemoryException {
        return getGroupStats(DEFAULT_GROUPS, stat);
    }
    long[] getGroupStats(String groupsName, List<String> stat) throws ImhotepOutOfMemoryException;

    // TODO:
    // long[][] getGroupStats(List<List<String>> stats) throws ImhotepOutOfMemoryException;

    /**
     * get the current total of a given metric for each group
     * Trailing groups with 0 values can cause the returned array to be shorter than the total number of groups.
     * @param stat the metric
     * @return an iterator with the metric values, indexed by group
     */
    default GroupStatsIterator getGroupStatsIterator(final List<String> stat) throws ImhotepOutOfMemoryException {
        return getGroupStatsIterator(DEFAULT_GROUPS, stat);
    }
    GroupStatsIterator getGroupStatsIterator(String groupsName, List<String> stat) throws ImhotepOutOfMemoryException;

    // TODO:
    // GroupStatsIterator getGroupStatsIterator(List<List<String>> stats) throws ImhotepOutOfMemoryException;

    /**
     * get an iterator over all (field, term, group, stat) tuples for the given fields
     * @param intFields list of int fields
     * @param stringFields list of string fields
     * @param stats list of stats to return in the FTGS. null means fall back to session stack.
     * @return an iterator. result is the same as after calling
     *          getFTGSIterator(new FTGSParams(intFields, stringFields, 0, -1, true, stats, StatsSortOrder.UNDEFINED));
     */
    default FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields, @Nullable final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        return getFTGSIterator(DEFAULT_GROUPS, intFields, stringFields, stats);
    }
    FTGSIterator getFTGSIterator(String groupsName, String[] intFields, String[] stringFields, @Nullable List<List<String>> stats) throws ImhotepOutOfMemoryException;

    /**
     * get an iterator over up to termLimit (field, term, group, stat) tuples for the given fields
     * @param intFields list of int fields
     * @param stringFields list of string fields
     * @param termLimit maximum number of terms that can be returned. 0 means no limit
     * @param stats list of stats to return in the FTGS. null means fall back to session stack.
     * @return an iterator. result is the same as after calling
     *          getFTGSIterator(new FTGSParams(intFields, stringFields, termLimit, -1, true, stats, StatsSortOrder.UNDEFINED));
     */
    default FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields, final long termLimit, @Nullable final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        return getFTGSIterator(DEFAULT_GROUPS, intFields, stringFields, termLimit, stats);
    }
    FTGSIterator getFTGSIterator(String groupsName, String[] intFields, String[] stringFields, long termLimit, @Nullable List<List<String>> stats) throws ImhotepOutOfMemoryException;

    /**
     * get an iterator over (field, term, group, stat) for top termLimit terms sorted by stats in sortStat in order given in sortOrder
     * @param intFields list of int fields
     * @param stringFields list of string fields
     * @param termLimit maximum numbers of terms that can be returned. 0 means no limit
     * @param sortStat the index of stats to get the top terms. No sorting is done if the value is negative
     * @param stats list of stats to return in the FTGS. null means fall back to session stack.
     * @param statsSortOrder the order (Ascending/Descending) of stats. Ties within terms are resolved in the same order ( same as in StatsSortOrder )
     * @return an iterator. result is the same as after calling
     *          getFTGSIterator(new FTGSParams(intFields, stringFields, termLimit, sortStat, true, stats, statsSortOrder));
     */
    default FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields, final long termLimit, final int sortStat, @Nullable final List<List<String>> stats, final StatsSortOrder statsSortOrder) throws ImhotepOutOfMemoryException {
        return getFTGSIterator(DEFAULT_GROUPS, intFields, stringFields, termLimit, sortStat, stats, statsSortOrder);
    }
    FTGSIterator getFTGSIterator(String groupsName, String[] intFields, String[] stringFields, long termLimit, int sortStat, @Nullable List<List<String>> stats, StatsSortOrder statsSortOrder) throws ImhotepOutOfMemoryException;

    default FTGSIterator getSubsetFTGSIterator(final Map<String, long[]> intFields, final Map<String, String[]> stringFields, @Nullable final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        return getSubsetFTGSIterator(DEFAULT_GROUPS, intFields, stringFields, stats);
    }
    FTGSIterator getSubsetFTGSIterator(String groupsName, Map<String, long[]> intFields, Map<String, String[]> stringFields, @Nullable List<List<String>> stats) throws ImhotepOutOfMemoryException;

    /**
     * get an iterator over some subset (maybe all) of (field, term, group, stat) tuples for the given fields
     * tuples are always sorted by field and groups within term are always sorted too.
     *
     * Result depends on values of 'termLimit, sortStat, sorted' params:
     *
     * <table summary ="description of params">
     * <tr><th>No.</th><th>termLimit</th><th>sortStat</th><th>sorted</th><th>StatsSortOrder</th>
     *     <th>description</th></tr>
     * <tr><th>1</th><th>0</th><th>any</th><th>true</th><th>UNDEFINED</th>
     *     <th>all terms in all fields returned, terms within each field are sorted by term value</th></tr>
     * <tr><th>2</th><th>0</th><th>any</th><th>false</th><th>UNDEFINED</th>
     *     <th>all terms in all fields returned, terms within each field go in any order</th></tr>
     * <tr><th>3</th><th>&gt; 0</th><th>&gt;= 0</th><th>true</th><th>ASCENDING</th>
     *     <th>for each (field, group) pair only (up to) 'termLimit' tuples with biggest
     *     sortStat metric value appear in result. Terms within each field are sorted by term value.
     *     Metric ties are resolved by terms</th></tr>
     * <tr><th>4</th><th>&gt; 0</th><th>&gt;= 0</th><th>true</th><th>DESCENDING</th>
     *     <th>for each (field, group) pair only (up to) 'termLimit' tuples with smallest
     *     sortStat metric value appear in result. Terms within each field are sorted by term value.
     *     Metric ties are resolved by decreasing order of terms</th></tr>
     * <tr><th>5</th><th>&gt; 0</th><th>&gt;= 0</th><th>false</th><th>ASCENDING</th>
     *     <th>same as 3, but terms can go in any order</th></tr>
     * <tr><th>6</th><th>&gt; 0</th><th>&gt;= 0</th><th>false</th><th>DESCENDING</th>
     *     <th>same as 4, but terms can go in any order</th></tr>
     * <tr><th>7</th><th>&gt; 0</th><th>&lt; 0</th><th>true</th><th>UNDEFINED</th>
     *     <th>iterate through tuples sorted by term value until 'termLimit' terms are emitted</th></tr>
     * <tr><th>8</th><th>&gt; 0</th><th>&lt; 0</th><th>false</th><th>UNDEFINED</th>
     *     <th>return any 'termLimit' terms from any fields.
     *     Total number of terms are 'termLimit'. Terms within field are unsorted.
     *     (note: it's not FIRST 'termLimit' terms in first fields in any order,
     *     but ANY 'termLimit' terms in any field)</th></tr>
     * <tr><th>9</th><th>&lt; 0</th><th>any</th><th>any</th><th>any</th>
     *     <th>IllegalArgumentException</th></tr>
     * <tr><th>10</th><th>any</th><th>&lt; 0</th><th>any</th><th>ASCENDING/DESCENDING</th>
     *     <th>IllegalArgumentException</th></tr>
     * </table>
     * @param params params for resulting iterator
     * @return an iterator
     */
    default FTGSIterator getFTGSIterator(final FTGSParams params) throws ImhotepOutOfMemoryException {
        return getFTGSIterator(DEFAULT_GROUPS, params);
    }
    FTGSIterator getFTGSIterator(String groupsName, FTGSParams params) throws ImhotepOutOfMemoryException;

    /**
     * Get distinct terms count per group.
     * @param field the field to use
     * @param isIntField whether the field is int or string type
     * @return an iterator with the distinct terms count, indexed by group
     */
    default GroupStatsIterator getDistinct(final String field, final boolean isIntField) {
        return getDistinct(DEFAULT_GROUPS, field, isIntField);
    }
    GroupStatsIterator getDistinct(String groupsName, String field, boolean isIntField);

    /**
     * apply the list of remap rules to remap documents into a different group. Preconditions:
     *
     * <ul>
     *     <li>Each rule has a different targetGroup
     *     <li>All targetGroups are positive
     *     <li>All inequality conditions have the potential to be matched -- i.e., there is not an earlier inequality
     *         condition in the same GroupMultiRemapRule that targets the same field with a greater term.
     *     <li>All equality conditions have the potential to be matched -- i.e., there is not an earlier equality
     *         condition in the same GroupMultiRemapRule that targets the same field with the same term
     * </ul>
     *
     * After the regroup operation:
     *
     * <ul>
     *     <li>If a document was matched by some rule (its original group was equal to the rule's targetGroup)
     *     then its new group is the positiveGroup of the earliest condition in the rule that matches the document,
     *     or the negativeGroup of the rule, depending on whether it matched any rules
     *     <li>Otherwise, the document's new group is 0.
     * </ul>
     *
     * @param rawRules list of remap rules
     * @return the number of groups after applying the regroup
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to run out of memory
     * @throws IllegalArgumentException if there are duplicate targetGroups, non-positive targetGroups, or regroup
     *                                  conditions do not meet the above prescribed requirements
     */
    default int regroup(final GroupMultiRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        return regroup(RegroupParams.DEFAULT, rawRules);
    }
    int regroup(RegroupParams regroupParams, GroupMultiRemapRule[] rawRules) throws ImhotepOutOfMemoryException;

    default int regroup(final int numRawRules, final Iterator<GroupMultiRemapRule> rawRules) throws ImhotepOutOfMemoryException {
        return regroup(RegroupParams.DEFAULT, numRawRules, rawRules);
    }
    int regroup(RegroupParams regroupParams, int numRawRules, Iterator<GroupMultiRemapRule> rawRules) throws ImhotepOutOfMemoryException;

    default int regroup(final GroupMultiRemapRule[] rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        return regroup(RegroupParams.DEFAULT, rawRules, errorOnCollisions);
    }
    int regroup(RegroupParams regroupParams, GroupMultiRemapRule[] rawRules, boolean errorOnCollisions) throws ImhotepOutOfMemoryException;

    default int regroup(final int numRawRules, final Iterator<GroupMultiRemapRule> rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        return regroup(RegroupParams.DEFAULT, numRawRules, rawRules, errorOnCollisions);
    }
    int regroup(RegroupParams regroupParams, int numRawRules, Iterator<GroupMultiRemapRule> rawRules, boolean errorOnCollisions) throws ImhotepOutOfMemoryException;

    /**
     * apply this query to the dataset and regroup based on whether or not a document matches the query
     *
     * After the regroup operation:
     *
     * <ul>
     *      <li>All documents in the rule's targetGroup that were matched by the rule's query will now be in the rule's positiveGroup</li>
     *      <li>All documents in the rule's targetGroup that were not matched by the rule's query will now be in the rule's negativeGroup</li>
     *      <li>All documents not in the rule's targetGroup will remain in the same group they were in before the regroup operation</li>
     * </ul>
     *
     * @param rule the query to execute and the group parameters
     * @return the number of groups after applying the regroup
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to run out of memory
     */
    default int regroup(final QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        return regroup(RegroupParams.DEFAULT, rule);
    }
    int regroup(RegroupParams regroupParams, QueryRemapRule rule) throws ImhotepOutOfMemoryException;

    /**
     * a regroup for doing OR queries over int fields
     * @param field the int field
     * @param terms sorted list of terms, any doc matching any of these terms will be remapped
     * @param targetGroup group to map from
     * @param negativeGroup group into which to map docs that contain none of the terms
     * @param positiveGroup group into witch to map docs that contain any of the terms
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to run out of memory
     */
    default void intOrRegroup(final String field, final long[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        intOrRegroup(RegroupParams.DEFAULT, field, terms, targetGroup, negativeGroup, positiveGroup);
    }
    void intOrRegroup(RegroupParams regroupParams, String field, long[] terms, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException;

    /**
     * a regroup for doing OR queries over string fields
     * @param field the string field
     * @param terms sorted list of terms, any doc matching any of these terms will be remapped
     * @param targetGroup group to map from
     * @param negativeGroup group into which to map docs that contain none of the terms
     * @param positiveGroup group into witch to map docs that contain any of the terms
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to run out of memory
     */
    default void stringOrRegroup(final String field, final String[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        stringOrRegroup(RegroupParams.DEFAULT, field, terms, targetGroup, negativeGroup, positiveGroup);
    }
    void stringOrRegroup(RegroupParams regroupParams, String field, String[] terms, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException;

    /**
     * a regroup for doing regex filtering over string fields
     * @param field the string field
     * @param regex the regex to test the terms against
     * @param targetGroup group to map from
     * @param negativeGroup group into which to map docs that don't have any terms matching the regex
     * @param positiveGroup group into witch to map docs that contain terms that match the regex
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to run out of memory
     */
    default void regexRegroup(final String field, final String regex, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        regexRegroup(RegroupParams.DEFAULT, field, regex, targetGroup, negativeGroup, positiveGroup);
    }
    void regexRegroup(RegroupParams regroupParams, String field, String regex, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException;


    /**
     * perform a random regrouping of documents based on a specific field
     * this is done by applying the salt to each term, hashing, and mapping the hash to a value between 0 and 1
     * all terms with value &lt; p go in negativeGroup, all terms with value &gt;= p go in positiveGroup
     * the actual grouping is only as random as the salt
     *
     * @param field the field to use
     * @param isIntField whether the field is int or string type
     * @param salt the salt to use
     * @param p the minimum value to go into positiveGroup
     * @param targetGroup the group to apply the random regroup to
     * @param negativeGroup the group where terms with values &lt; p will go
     * @param positiveGroup the group where terms with values &gt;= p will go
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to go out of memory
     */
    default void randomRegroup(final String field, final boolean isIntField, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        randomRegroup(RegroupParams.DEFAULT, field, isIntField, salt, p, targetGroup, negativeGroup, positiveGroup);
    }
    void randomRegroup(RegroupParams regroupParams, String field, boolean isIntField, String salt, double p, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException;


    /**
     * Same as <code>randomRegroup</code> but regrouping is based on metric value
     * @param stat the metric
     * @param salt the salt to use
     * @param p the minimum value to go into positiveGroup
     * @param targetGroup the group to apply the random regroup to
     * @param negativeGroup the group where terms with values &lt; p will go
     * @param positiveGroup the group where terms with values &gt;= p will go
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to go out of memory
     */
    default void randomMetricRegroup(final List<String> stat, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        randomMetricRegroup(RegroupParams.DEFAULT, stat, salt, p, targetGroup, negativeGroup, positiveGroup);
    }
    void randomMetricRegroup(RegroupParams regroupParams, List<String> stat, String salt, double p, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException;

    /**
     * Performs a random regroup, except instead of a binary decision, partitions into groups based on a percentage map.
     *
     * The percentage map is an array of split points for the result groups. Each document whose random value is less
     * than or equal to percentages[i] will be mapped to groups[i], and all remaining documents will be mapped to
     * groups[groups.length - 1]. Therefore, it is required that the percentage map be (1) in ascending order between
     * 0.0 and 1.0, and (2) one element shorter than resultGroups.
     *
     * For example, if percentages = [0.40, 0.80] and resultGroups = [3, 4, 6], then 40% of documents currently in the
     * target group will be placed in group 3, 40% will be placed in group 4, and 20% will be placed in group 6.
     *
     * All other behavior is the same as randomRegroup(). As always, the grouping is only as random as the salt.
     *
     * @param stat the metric
     * @param salt The salt to use
     * @param targetGroup The group to apply the random regroup to
     * @param percentages The group cutoff percentages, works together with resultGroups
     * @param resultGroups The groups to regroup into, works together with percentages
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to go out of memory
     */
    default void randomMetricMultiRegroup(final List<String> stat, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        randomMetricMultiRegroup(RegroupParams.DEFAULT, stat, salt, targetGroup, percentages, resultGroups);
    }
    void randomMetricMultiRegroup(RegroupParams regroupParams, List<String> stat, String salt, int targetGroup, double[] percentages, int[] resultGroups) throws ImhotepOutOfMemoryException;

    default int metricRegroup(final List<String> stat, final long min, final long max, final long intervalSize) throws ImhotepOutOfMemoryException {
        return metricRegroup(RegroupParams.DEFAULT, stat, min, max, intervalSize);
    }
    int metricRegroup(RegroupParams regroupParams, List<String> stat, long min, long max, long intervalSize) throws ImhotepOutOfMemoryException;

    default int metricRegroup(final List<String> stat, final long min, final long max, final long intervalSize, final boolean noGutters) throws ImhotepOutOfMemoryException {
        return metricRegroup(RegroupParams.DEFAULT, stat, min, max, intervalSize, noGutters);
    }
    int metricRegroup(RegroupParams regroupParams, List<String> stat, long min, long max, long intervalSize, boolean noGutters) throws ImhotepOutOfMemoryException;

    /**
     * Unconditional remapping from one groups to another groups.
     * All documents with group fromGroups[i] remap to group toGroups[i]
     *      length of fromGroups and toGroups must be equal
     *      All values in fromGroups must be unique.
     *
     * @param fromGroups target groups
     * @param toGroups resulting groups
     * @param filterOutNotTargeted what to do with groups that are not in fromGroups
     *                             true - remap to zero group
     *                             false - leave as is.
     * @return the number of groups after applying the regroup
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to run out of memory
     */
    default int regroup(final int[] fromGroups, final int[] toGroups, final boolean filterOutNotTargeted) throws ImhotepOutOfMemoryException {
        return regroup(RegroupParams.DEFAULT, fromGroups, toGroups, filterOutNotTargeted);
    }
    int regroup(final RegroupParams regroupParams, final int[] fromGroups, final int[] toGroups, final boolean filterOutNotTargeted) throws ImhotepOutOfMemoryException;

    /**
     * Filters documents based on whether or not the given stat is within the specified range.
     * If negate=false, then documents with the metric inside the range are kept in their
     *     present groups, and documents outside of the range are moved to group 0.
     * If negate=true, then documents with the metric inside the range are moved to group 0,
     *     and documents outside of the range are kept in their present groups.
     *
     * @param stat metric to filter based on
     * @param min minimum value (inclusive) of the metric range
     * @param max maximum value (inclusive!) of the metric range
     * @param negate whether the range is being kept, or the negation of the range is being kept
     * @return the number of groups after applying the regroup
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to run out of memory
     */
    default int metricFilter(final List<String> stat, final long min, final long max, final boolean negate) throws ImhotepOutOfMemoryException {
        return metricFilter(RegroupParams.DEFAULT, stat, min, max, negate);
    }
    int metricFilter(RegroupParams regroupParams, List<String> stat, long min, long max, boolean negate) throws ImhotepOutOfMemoryException;

    /**
     * Move documents based on whether or not the given stat is within the specified range.
     *
     * @param stat metric to filter based on
     * @param min minimum value (inclusive) of the metric range
     * @param max maximum value (inclusive!) of the metric range
     * @param targetGroup the group to affect
     * @param negativeGroup the group to move documents outside of the range to
     * @param positiveGroup the group to move documents inside the range to
     * @return the number of groups after applying the regroup
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to run out of memory
     */
    default int metricFilter(final List<String> stat, final long min, final long max, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        return metricFilter(RegroupParams.DEFAULT, stat, min, max, targetGroup, negativeGroup, positiveGroup);
    }
    int metricFilter(RegroupParams regroupParams, List<String> stat, long min, long max, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException;

    /**
     * Return a list of the top k terms for a field, sorted by document frequency descending.
     *
     * This method will be significantly faster than pushing count() and doing FTGS iteration but the results
     * are not guaranteed to be exact.
     *
     * Additionally, this method is not guaranteed to respect values of k larger than 1000.
     * 
     * @param field the field to retrieve top terms for
     * @param isIntField whether or not the field is an int field
     * @param k the desired number of terms
     * @return approximate top terms
     */
    List<TermCount> approximateTopTerms(String field, boolean isIntField, int k);

    /**
     * Combine the different named input groups together on a per-document using the specified operator.
     * The resulting groups will be stored in the specified outputGroups.
     *
     * All input groups must only have groups that are 0 or 1. They must also be ConstantGroupLookup or BitSetGroupLookups.
     * If the operation is AND or OR and there are fewer than 2 arguments, an IllegalArgumentException will be thrown.
     * If the operation is NOT and there is not exactly 1 argument, an IllegalArgumentException will be thrown.
     *
     * @param inputGroups collection of named input groups
     * @param operation operator to use
     * @param outputGroups where to store the result
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to run out of memory
     */
    void consolidateGroups(final List<String> inputGroups, final Operator operation, final String outputGroups) throws ImhotepOutOfMemoryException;

    /**
     * Delete the given groups
     */
    void deleteGroups(final List<String> groupsToDelete);

    /**
     * push the metric specified by statName
     * @param statName the metric to push
     * @return the number of stats after pushing this metric
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to run out of memory
     */
    @Deprecated
    int pushStat(String statName) throws ImhotepOutOfMemoryException;

    /**
     * push the metrics specified by statNames
     * @param statNames the metrics to push
     * @return the number of stats after pushing the last metric
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to run out of memory
     */
    @Deprecated
    int pushStats(List<String> statNames) throws ImhotepOutOfMemoryException;

    /**
     * pop the most recently-added metric
     * @return the number of stats after popping the metric
     */
    @Deprecated
    int popStat();

    /**
     * @return the number of stats currently on the stack.
     */
    int getNumStats();

    /**
     * @return number of groups including zero group (maxGroup+1)
     */
    default int getNumGroups() {
        return getNumGroups(ImhotepSession.DEFAULT_GROUPS);
    }
    int getNumGroups(String groupsName);

    /**
     * close the session and free up any associated resources
     */
    void close();

    /**
     * reset groups to their original state (all documents in group 1)
     */
    default void resetGroups() throws ImhotepOutOfMemoryException {
        resetGroups(DEFAULT_GROUPS);
    }
    void resetGroups(String groupsName) throws ImhotepOutOfMemoryException;

    /** Returns the number of docs in the shards handled by this session */
    long getNumDocs();

    /**
     * ImhotepSession is collecting some performance and execution statistics.
     * @return performance statistics collected since last reset (or since creation of the session)
     */
    PerformanceStats getPerformanceStats(final boolean reset);

    /**
     * Effect is same as calling getPerformanceStats(...) and then close()
     * One method is because we can save one request to a server in case of remote session
     * @return same as  {@link #getPerformanceStats} or null if session is already closed.
     */
    PerformanceStats closeAndGetPerformanceStats();
}
