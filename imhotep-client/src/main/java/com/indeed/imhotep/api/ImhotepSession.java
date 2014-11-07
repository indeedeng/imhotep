/*
 * Copyright (C) 2014 Indeed Inc.
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
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.TermCount;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@NotThreadSafe
public interface ImhotepSession extends Closeable {
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
     * @param stat the index of the metric
     * @return an array with the metric values, indexed by group
     */
    long[] getGroupStats(int stat);

    /**
     * get an iterator over all (field, term, group, stat) tuples for the given fields
     * @param intFields list of int fields
     * @param stringFields list of string fields
     * @return an iterator
     */
    FTGSIterator getFTGSIterator(String[] intFields, String[] stringFields);

    FTGSIterator getSubsetFTGSIterator(Map<String, long[]> intFields, Map<String, String[]> stringFields);

    DocIterator getDocIterator(String[] intFields, String[] stringFields) throws ImhotepOutOfMemoryException;

    RawFTGSIterator[] getFTGSIteratorSplits(String[] intFields, String[] stringFields);

    /**
     * note: this call is weird.
     * it is intended to be called numSplits times, once with each splitIndex from [0..n)
     * if it is not called numSplits times the returned iterators will deadlock
     * if it is called with different values for numSplits it will throw an error
     * if it is called with the same value for splitIndex twice it will throw an error
     * if you are a client of imhotep this is not the call you are looking for
     *
     * @param intFields list of int fields
     * @param stringFields list of string fields
     * @param splitIndex index of the split you want
     * @param numSplits total number of splits
     * @return iterator
     */
    RawFTGSIterator getFTGSIteratorSplit(String[] intFields, String[] stringFields, int splitIndex, int numSplits);

    RawFTGSIterator getSubsetFTGSIteratorSplit(Map<String, long[]> intFields, Map<String, String[]> stringFields, int splitIndex, int numSplits);

    /**
     * this is only really here to be called on ImhotepRemoteSession by RemoteImhotepMultiSession
     */
    RawFTGSIterator mergeFTGSSplit(String[] intFields, String[] stringFields, String sessionId, InetSocketAddress[] nodes, int splitIndex);

    RawFTGSIterator mergeSubsetFTGSSplit(Map<String, long[]> intFields, Map<String, String[]> stringFields, String sessionId, InetSocketAddress[] nodes, int splitIndex);

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
    int regroup(GroupMultiRemapRule[] rawRules) throws ImhotepOutOfMemoryException;

    int regroup(int numRawRules, Iterator<GroupMultiRemapRule> rawRules) throws ImhotepOutOfMemoryException;

    int regroup(GroupMultiRemapRule[] rawRules, boolean errorOnCollisions) throws ImhotepOutOfMemoryException;

    int regroup(int numRawRules, Iterator<GroupMultiRemapRule> rawRules, boolean errorOnCollisions) throws ImhotepOutOfMemoryException;

    /**
     * apply the list of remap rules to remap documents into a different group. Preconditions:
     *
     * <ul>
     *     <li>Each rule has a different targetGroup
     *     <li>All targetGroups are non-negative
     * </ul>
     *
     * After the regroup operation:
     *
     * <ul>
     *     <li>If a document was matched by some rule (its original group was equal to the rule's targetGroup)
     *     then its new group is the rule's positiveGroup or negativeGroup, depending on whether it matched
     *     the rule's condition.
     *     <li>Otherwise, the document's new group is 0.
     * </ul>
     *
     * @param rawRules list of remap rules
     * @return the number of groups after applying the regroup
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to run out of memory
     */
    int regroup(GroupRemapRule[] rawRules) throws ImhotepOutOfMemoryException;

    int regroup2(int numRawRules, Iterator<GroupRemapRule> iterator) throws ImhotepOutOfMemoryException;

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
    int regroup(QueryRemapRule rule) throws ImhotepOutOfMemoryException;

    /**
     * a regroup for doing OR queries over int fields
     * @param field the int field
     * @param terms sorted list of terms, any doc matching any of these terms will be remapped
     * @param targetGroup group to map from
     * @param negativeGroup group into which to map docs that contain none of the terms
     * @param positiveGroup group into witch to map docs that contain any of the terms
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to run out of memory
     */
    void intOrRegroup(String field, long[] terms, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException;

    /**
     * a regroup for doing OR queries over string fields
     * @param field the string field
     * @param terms sorted list of terms, any doc matching any of these terms will be remapped
     * @param targetGroup group to map from
     * @param negativeGroup group into which to map docs that contain none of the terms
     * @param positiveGroup group into witch to map docs that contain any of the terms
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to run out of memory
     */
    void stringOrRegroup(String field, String[] terms, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException;

    /**
     * perform a random regrouping of documents based on a specific field
     * this is done by applying the salt to each term, hashing, and mapping the hash to a value between 0 and 1
     * all terms with value < p go in negativeGroup, all terms with value >= p go in positiveGroup
     * the actual grouping is only as random as the salt
     *
     * @param field the field to use
     * @param isIntField whether the field is int or string type
     * @param salt the salt to use
     * @param p the minimum value to go into positiveGroup
     * @param targetGroup the group to apply the random regroup to
     * @param negativeGroup the group where terms with values < p will go
     * @param positiveGroup the group where terms with values >= p will go
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to go out of memory
     */
    void randomRegroup(String field, boolean isIntField, String salt, double p, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException;

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
     * @param field Field to split randomly over
     * @param isIntField true if 'field' is an integer field, false if it is a string field
     * @param salt The salt to use
     * @param targetGroup The group to apply the random regroup to
     * @param percentages The group cutoff percentages, works together with resultGroups
     * @param resultGroups The groups to regroup into, works together with percentages
     */
    void randomMultiRegroup(String field, boolean isIntField, String salt, int targetGroup, double[] percentages, int[] resultGroups) throws ImhotepOutOfMemoryException;

    int metricRegroup(int stat, long min, long max, long intervalSize) throws ImhotepOutOfMemoryException;

    int metricRegroup(int stat, long min, long max, long intervalSize, boolean noGutters) throws ImhotepOutOfMemoryException;

    int metricRegroup2D(int xStat, long xMin, long xMax, long xIntervalSize,
                        int yStat, long yMin, long yMax, long yIntervalSize) throws ImhotepOutOfMemoryException;

    int metricFilter(int stat, long min, long max, boolean negate) throws ImhotepOutOfMemoryException;

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
     * push the metric specified by statName
     * @param statName the metric to push
     * @return the number of stats after pushing this metric
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to run out of memory
     */
    int pushStat(String statName) throws ImhotepOutOfMemoryException;

    /**
     * push the metrics specified by statNames
     * @param statNames the metrics to push
     * @return the number of stats after pushing the last metric
     * @throws ImhotepOutOfMemoryException if performing this operation would cause imhotep to run out of memory
     */
    int pushStats(List<String> statNames) throws ImhotepOutOfMemoryException;

    /**
     * pop the most recently-added metric
     * @return the number of stats after popping the metric
     */
    int popStat();

    /**
     * @return the number of stats currently on the stack.
     */
    int getNumStats();

    /**
     * @return number of groups including zero group (maxGroup+1)
     */
    int getNumGroups();

    /**
     * create a per-document dynamic metric
     * @param name the name of the metric to create
     * @throws ImhotepOutOfMemoryException in case there's not enough memory
     */
    void createDynamicMetric(String name) throws ImhotepOutOfMemoryException;

    /**
     * add a per-group constant to each element of a dynamic metric, using saturating arithmetic
     * @param name the name of the metric to update
     * @param deltas an array of constant values to add for each group
     * @throws ImhotepOutOfMemoryException in case there's not enough memory
     */
    void updateDynamicMetric(String name, int[] deltas) throws ImhotepOutOfMemoryException;

    /**
     * Adjusts the given dynamic metric on a per-document basis where the delta for each condition that matches
     * is summed up and applied.
     * The group that a document is in is irrelevant.
     * Does not currently support inequality conditions.
     * @param name the name of the metric to update
     * @param conditions conditions to match against
     * @param deltas deltas to adjust document by if the corresponding condition matches
     */
    void conditionalUpdateDynamicMetric(String name, RegroupCondition[] conditions, int[] deltas);

    void groupConditionalUpdateDynamicMetric(String name, int[] groups, RegroupCondition[] conditions, int[] deltas);

    /**
     * close the session and free up any associated resources
     */
    void close();

    /**
     * reset groups to their original state (all documents in group 1)
     */
    void resetGroups() throws ImhotepOutOfMemoryException;

    /**
     * Rebuilds the Indexes and removes all docs in group 0. May make 
     * future FTGS passes more efficent.
     * @throws ImhotepOutOfMemoryException 
     */
    void rebuildAndFilterIndexes(List<String> intFields, List<String> stringFields) throws ImhotepOutOfMemoryException;
}
