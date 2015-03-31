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
 package com.indeed.imhotep.local;

import com.google.common.collect.ComparisonChain;
import com.indeed.util.core.sort.Quicksortable;
import com.indeed.util.core.sort.Quicksortables;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.simple.SimpleIntTermIterator;
import com.indeed.flamdex.simple.SimpleStringTermIterator;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author jwolfe
 */
class MultiRegroupInternals {
    static int countRemapConditions(GroupMultiRemapRule[] rules) {
        int conditionCount = 0;
        for (GroupMultiRemapRule rule : rules) {
            conditionCount += rule.conditions.length;
        }
        return conditionCount;
    }

    static int findMaxGroup(GroupMultiRemapRule[] rules) {
        int maxGroup = 0;
        for (GroupMultiRemapRule rule : rules) {
            for (int positiveGroup : rule.positiveGroups) {
                maxGroup = Math.max(maxGroup, positiveGroup);
            }
            maxGroup = Math.max(maxGroup, rule.targetGroup);
            maxGroup = Math.max(maxGroup, rule.negativeGroup);
        }
        return maxGroup;
    }

    public static int findMaxIntermediateGroup(GroupMultiRemapRule[] rules) {
        int max = 0;
        for (GroupMultiRemapRule rule : rules) {
            max = Math.max(max, rule.conditions.length);
        }
        return max;
    }


    /*
     * Reorders the variables prefixed with "sorted" by field, intType, inequality, targetGroup, and then position within their rule
     */
    static void reorderRegroupConditions(final GroupMultiRemapRule[] rules, int numConditions, final RegroupCondition[] sortedConditions, final int[] sortedPositiveGroups, final int[] sortedInternalIndices, final int[] sortedRuleIndices) {
        Quicksortables.sort(new Quicksortable() {
            @Override
            public void swap(int i, int j) {
                Quicksortables.swap(sortedConditions, i, j);
                Quicksortables.swap(sortedPositiveGroups, i, j);
                Quicksortables.swap(sortedInternalIndices, i, j);
                Quicksortables.swap(sortedRuleIndices, i, j);
            }

            @Override
            public int compare(int i, int j) {
                return ComparisonChain.start()
                        .compare(sortedConditions[i].field, sortedConditions[j].field)
                        .compareFalseFirst(sortedConditions[i].intType, sortedConditions[j].intType)
                        .compareFalseFirst(sortedConditions[i].inequality, sortedConditions[j].inequality)
                        .compare(rules[sortedRuleIndices[i]].targetGroup, rules[sortedRuleIndices[j]].targetGroup)
                        .compare(sortedInternalIndices[i], sortedInternalIndices[j])
                        .result();
            }
        }, numConditions);
    }

    static void reorderOnTerm(final int start, int end, final boolean intType, final RegroupCondition[] sortedConditions, final int[] sortedPositiveGroups, final int[] sortedInternalIndices, final int[] sortedRuleIndices) {
        Quicksortables.sort(new Quicksortable() {
            @Override
            public void swap(int i, int j) {
                Quicksortables.swap(sortedConditions, start + i, start + j);
                Quicksortables.swap(sortedPositiveGroups, start + i, start + j);
                Quicksortables.swap(sortedInternalIndices, start + i, start + j);
                Quicksortables.swap(sortedRuleIndices, start + i, start + j);
            }

            @Override
            public int compare(int i, int j) {
                if (intType) {
                    return ComparisonChain.start()
                            .compare(sortedConditions[start + i].intTerm, sortedConditions[start + j].intTerm)
                            .result();
                } else {
                    return ComparisonChain.start()
                            .compare(sortedConditions[start + i].stringTerm, sortedConditions[start + j].stringTerm)
                            .result();
                }
            }
        }, end - start);
    }

    /*
     * Verifies that targetGroups are unique and are >= 1, and returns the highest group found.
     */
    static int validateTargets(GroupMultiRemapRule[] rules) {
        final int[] targetGroups = new int[rules.length];
        for (int i = 0; i < rules.length; i++) {
            targetGroups[i] = rules[i].targetGroup;
        }

        // Verify no duplicates
        Arrays.sort(targetGroups);
        if (targetGroups[0] <= 0) {
            throw new IllegalArgumentException("All groups must be >= 1");
        }
        for (int i = 0; i < targetGroups.length - 1; i++) {
            if (targetGroups[i] == targetGroups[i+1]) {
                throw new IllegalArgumentException("Cannot have duplicate target groups");
            }
        }
        return targetGroups[targetGroups.length - 1];
    }

    /*
     * Verifies that there are no unmatchable terms
     * @throws IllegalArgumentException if the same rule has multiple conditions targeting
     *                                  the same term in the same field
     */
    static void validateEqualitySplits(GroupMultiRemapRule[] rules) {
        for (GroupMultiRemapRule rule : rules) {
            final RegroupCondition[] sortedConditions = Arrays.copyOf(rule.conditions, rule.conditions.length);
            sortConditions(sortedConditions);
            for (int i = 0; i < sortedConditions.length-1; i++) {
                final RegroupCondition s1 = sortedConditions[i];
                final RegroupCondition s2 = sortedConditions[i+1];
                if (s1.field.equals(s2.field)
                        && !s1.inequality
                        && !s2.inequality
                        && (s1.intType == s2.intType)) {
                    if (s1.intType) {
                        if (s1.intTerm == s2.intTerm) {
                            throw new IllegalArgumentException("Duplicate equality split term "+s1.intTerm+" in int field "+s1.field);
                        }
                    } else {
                        if (s1.stringTerm.equals(s2.stringTerm)) {
                            throw new IllegalArgumentException("Duplicate equality split term \""+s1.stringTerm+"\" in string field "+s1.field);
                        }
                    }
                }
            }
        }
    }

    private static void sortConditions(final RegroupCondition[] sortedConditions) {
        Quicksortables.sort(new Quicksortable() {
            @Override
            public void swap(int i, int j) {
                final RegroupCondition tmp = sortedConditions[i];
                sortedConditions[i] = sortedConditions[j];
                sortedConditions[j] = tmp;
            }

            @Override
            public int compare(int i, int j) {
                final RegroupCondition s1 = sortedConditions[i];
                final RegroupCondition s2 = sortedConditions[j];
                final int r = ComparisonChain.start()
                        .compare(s1.field, s2.field)
                        .compareFalseFirst(s1.inequality, s2.inequality)
                        .compareFalseFirst(s1.intType, s2.intType)
                        .result();
                if (r != 0) {
                    return r;
                } else if (s1.intType) {
                    // Both are int type
                    return ComparisonChain.start()
                            .compare(s1.intTerm, s2.intTerm)
                            .result();
                } else {
                    // Both are string type
                    return ComparisonChain.start()
                            .compare(s1.stringTerm, s2.stringTerm)
                            .result();
                }
            }
        }, sortedConditions.length);
    }

    /*
     * Upon returning, barriers contains a sorted list of differentiation points for each targetGroup in this inequality split.
     * resultingIndex is a parallel array saying, for each entry in barriers, what condition index it corresponds to.
     * barrierLengths says to what extent the barriers for each group are full (i.e., the effective length)
     */
    static void formStringDividers(GroupMultiRemapRule[] rules, RegroupCondition[] allConditions, int[] internalConditionIndices, int[] ruleIndices, int fieldStartIndex, int conditionIndex, int[] barrierLengths, String[][] barriers, int[][] resultingIndex) {
        for (int ix = fieldStartIndex; ix < conditionIndex; ix++) {
            final GroupMultiRemapRule rule = rules[ruleIndices[ix]];
            final int targetGroup = rule.targetGroup;
            final RegroupCondition condition = allConditions[ix];
            final int currentIndex = barrierLengths[targetGroup];
            if (currentIndex == 0) {
                barriers[targetGroup] = new String[rule.conditions.length];
                resultingIndex[targetGroup] = new int[rule.conditions.length];
            }
            if (currentIndex == 0 || condition.stringTerm.compareTo(barriers[targetGroup][currentIndex-1]) > 0) {
                barriers[targetGroup][currentIndex] = condition.stringTerm;
                resultingIndex[targetGroup][currentIndex] = internalConditionIndices[ix];
                barrierLengths[targetGroup]++;
            } else {
                throw new IllegalArgumentException("String inequality conditions that can never be met.");
            }
        }
    }

    /*
     * Upon returning, barriers contains a sorted list of differentiation points for each targetGroup in this inequality split.
     * resultingIndex is a parallel array saying, for each entry in barriers, what condition index it corresponds to.
     * barrierLengths says to what extent the barriers for each group are full (i.e., the effective length)
     */
    static void formIntDividers(GroupMultiRemapRule[] rules, RegroupCondition[] allConditions, int[] internalConditionIndices, int[] ruleIndices, int fieldStartIndex, int conditionIndex, int[] barrierLengths, long[][] barriers, int[][] resultingIndex) {
        for (int ix = fieldStartIndex; ix < conditionIndex; ix++) {
            final GroupMultiRemapRule rule = rules[ruleIndices[ix]];
            final int targetGroup = rule.targetGroup;
            final RegroupCondition condition = allConditions[ix];
            final int currentIndex = barrierLengths[targetGroup];
            if (currentIndex == 0) {
                barriers[targetGroup] = new long[rule.conditions.length];
                resultingIndex[targetGroup] = new int[rule.conditions.length];
            }
            if (currentIndex == 0 || condition.intTerm > barriers[targetGroup][currentIndex-1]) {
                barriers[targetGroup][currentIndex] = condition.intTerm;
                resultingIndex[targetGroup][currentIndex] = internalConditionIndices[ix];
                barrierLengths[targetGroup]++;
            } else {
                throw new IllegalArgumentException("int inequality conditions that can never be met.");
            }
        }
    }

    private native static void nativeRemapDocsInTargetGroups(long nativeShardDataPtr,
                                                             int[] results,
                                                             long docListAddress,
                                                             int nDocs,
                                                             int[] remappings,
                                                             int placeHolderGroup);

    private static void remapDocsInTargetGroups(GroupLookup docIdToGroup,
                                                GroupLookup newLookup,
                                                int[] docIdBuf,
                                                DocIdStream docIdStream,
                                                int[] remappings,
                                                int placeHolderGroup) {
        while (true) {
            final int n = docIdStream.fillDocIdBuffer(docIdBuf);
            for (int i = 0; i < n; i++) {
                final int docId = docIdBuf[i];
                final int oldGroup = docIdToGroup.get(docId);
                if (oldGroup != 0) {
                    final int currentGroup = newLookup.get(docId);
                    if (placeHolderGroup > 0) {
                        if (currentGroup != placeHolderGroup) {
                            throw new IllegalArgumentException("Regrouping on a multi-valued field doesn't work correctly so the operation is rejected.");
                        }
                    }
                    newLookup.set(docId, Math.min(currentGroup, remappings[oldGroup]));
                }
            }
            if (n < docIdBuf.length) break;
        }
    }

    static void performStringMultiEqualityRegroup(GroupLookup docIdToGroup, GroupLookup newLookup, int[] docIdBuf, DocIdStream docIdStream, StringTermIterator termIterator, int[] remappings, String term, int placeHolderGroup) throws IOException {
        termIterator.reset(term);
        if (termIterator.next() && termIterator.term().equals(term)) {
            if (docIdToGroup instanceof MultiCache.MultiCacheGroupLookup &&
                termIterator instanceof SimpleStringTermIterator) {
                final SimpleStringTermIterator ssTermIterator = (SimpleStringTermIterator) termIterator;
                IntGroupLookup results = new IntGroupLookup(newLookup.getSession(), newLookup.size());
                newLookup.copyInto(results);
                nativeRemapDocsInTargetGroups(((MultiCache.MultiCacheGroupLookup) docIdToGroup).nativeShardDataPtr(),
                                              results.gibbyBits(),
                                              ssTermIterator.getDocListAddress() + ssTermIterator.getOffset(),
                                              ssTermIterator.docFreq(),
                                              remappings, placeHolderGroup);
                results.copyInto(newLookup);
            } else {
                docIdStream.reset(termIterator);
                remapDocsInTargetGroups(docIdToGroup, newLookup, docIdBuf, docIdStream, remappings, placeHolderGroup);
            }
        }
    }

    static void performIntMultiEqualityRegroup(GroupLookup docIdToGroup, GroupLookup newLookup, int[] docIdBuf, DocIdStream docIdStream, IntTermIterator termIterator, int[] remappings, long term, int placeHolderGroup) throws IOException {
        termIterator.reset(term);
        if (termIterator.next() && termIterator.term() == term) {
            if (docIdToGroup instanceof MultiCache.MultiCacheGroupLookup &&
                termIterator instanceof SimpleIntTermIterator) {
                final SimpleIntTermIterator ssTermIterator = (SimpleIntTermIterator) termIterator;
                IntGroupLookup results = new IntGroupLookup(newLookup.getSession(), newLookup.size());
                newLookup.copyInto(results);
                nativeRemapDocsInTargetGroups(((MultiCache.MultiCacheGroupLookup) docIdToGroup).nativeShardDataPtr(),
                                              results.gibbyBits(),
                                              ssTermIterator.getDocListAddress() + ssTermIterator.getOffset(),
                                              ssTermIterator.docFreq(),
                                              remappings, placeHolderGroup);
                results.copyInto(newLookup);
            } else {
                docIdStream.reset(termIterator);
                remapDocsInTargetGroups(docIdToGroup, newLookup, docIdBuf, docIdStream, remappings, placeHolderGroup);
            }
        }
    }

    static void internalMultiRegroup(GroupLookup docIdToGroup, GroupLookup newDocIdToGroup, int[] docIdBuf, FlamdexReader flamdexReader, GroupMultiRemapRule[] rules, int highestTarget, int numConditions, int placeholderGroup, int maxGroup, boolean errorOnCollisions) throws IOException, ImhotepOutOfMemoryException {
        // Make a bunch of parallel arrays so we can sort. Memory claimed in parallelArrayBytes.
        final RegroupCondition[] sortedConditions = new RegroupCondition[numConditions];
        final int[] sortedPositiveGroups = new int[numConditions];
        final int[] sortedInternalIndices = new int[numConditions];
        final int[] sortedRuleIndices = new int[numConditions];
        {
            int i = 0;
            for (int ruleIndex = 0; ruleIndex < rules.length; ruleIndex++) {
                final GroupMultiRemapRule rule = rules[ruleIndex];
                for (int conditionIndex = 0; conditionIndex < rule.conditions.length; conditionIndex++) {
                    final RegroupCondition condition = rule.conditions[conditionIndex];
                    sortedConditions[i] = condition;
                    sortedPositiveGroups[i] = rule.positiveGroups[conditionIndex];
                    sortedInternalIndices[i] = conditionIndex;
                    sortedRuleIndices[i] = ruleIndex;
                    i++;
                }
            }
        }

        // memory claimed in remappingBytes
        final int[] remappings = new int[maxGroup + 1];
        Arrays.fill(remappings, placeholderGroup);
        remappings[0] = 0;

        // Arrange in order of field, and within field in order of field type;
        reorderRegroupConditions(rules, numConditions, sortedConditions, sortedPositiveGroups, sortedInternalIndices, sortedRuleIndices);

        int fieldStartIndex = 0;
        String field = sortedConditions[0].field;
        boolean intType = sortedConditions[0].intType;
        boolean inequality = sortedConditions[0].inequality;
        for (int conditionIndex = 1; conditionIndex <= numConditions; conditionIndex++) {
            if ((conditionIndex != numConditions)
                    && sortedConditions[conditionIndex].field.equals(field)
                    && (sortedConditions[conditionIndex].intType == intType)
                    && (sortedConditions[conditionIndex].inequality == inequality)) {
                continue;
            }

            // End of field/field type clump; perform regroups.
            final DocIdStream docIdStream = flamdexReader.getDocIdStream();
            if (inequality) {
                // These two branches both rely on the fact that these parallel arrays
                // are sorted within this subarray by condition index within their rule.

                // Handle inequalities by finding, for each document, which range it falls
                // in to, and reassigning if it is an earlier rule than the one it presently
                // matches (or if it presently matches none at all).

                if (intType) {
                    // Memory for these claimed earlier (see maxInequalityBytes)
                    final int[] barrierLengths = new int[highestTarget+1];
                    final long[][] barriers = new long[highestTarget+1][];
                    final int[][] resultingIndex = new int[highestTarget+1][];
                    formIntDividers(rules, sortedConditions, sortedInternalIndices, sortedRuleIndices, fieldStartIndex, conditionIndex, barrierLengths, barriers, resultingIndex);

                    final IntTermIterator termIterator = flamdexReader.getIntTermIterator(field);
                    performIntMultiInequalityRegroup(docIdToGroup, newDocIdToGroup, docIdBuf, docIdStream, barrierLengths, barriers, resultingIndex, termIterator);
                    termIterator.close();
                } else {
                    // Memory for these claimed earlier (see maxInequalityBytes)
                    final int[] barrierLengths = new int[highestTarget+1];
                    final String[][] barriers = new String[highestTarget+1][];
                    final int[][] resultingIndex = new int[highestTarget+1][];
                    formStringDividers(rules, sortedConditions, sortedInternalIndices, sortedRuleIndices, fieldStartIndex, conditionIndex, barrierLengths, barriers, resultingIndex);

                    final StringTermIterator termIterator = flamdexReader.getStringTermIterator(field);
                    performStringMultiInequalityRegroup(docIdToGroup, newDocIdToGroup, docIdBuf, docIdStream, barrierLengths, barriers, resultingIndex, termIterator);
                    termIterator.close();
                }
            } else {
                // Handle term splits by going to the term directly and applying the rule.
                reorderOnTerm(fieldStartIndex, conditionIndex, intType, sortedConditions, sortedPositiveGroups, sortedInternalIndices, sortedRuleIndices);
                if (intType) {
                    final IntTermIterator termIterator = flamdexReader.getIntTermIterator(field);
                    long currentTerm = sortedConditions[fieldStartIndex].intTerm;
                    int termStartIndex = fieldStartIndex;
                    final int fieldEndIndex = conditionIndex;
                    for (int ix = fieldStartIndex; ix <= fieldEndIndex; ix++) {
                        if (ix != fieldEndIndex) {
                            if (sortedConditions[ix].intTerm == currentTerm) {
                                final int targetGroup = rules[sortedRuleIndices[ix]].targetGroup;
                                remappings[targetGroup] = sortedInternalIndices[ix];
                                continue;
                            }
                        }

                        performIntMultiEqualityRegroup(docIdToGroup, newDocIdToGroup, docIdBuf, docIdStream, termIterator, remappings, currentTerm, errorOnCollisions ? placeholderGroup: -1);

                        for (int ix2 = termStartIndex; ix2 < ix; ix2++) {
                            final int targetGroup = rules[sortedRuleIndices[ix2]].targetGroup;
                            remappings[targetGroup] = placeholderGroup;
                        }

                        if (ix != fieldEndIndex) {
                            termStartIndex = ix;
                            currentTerm = sortedConditions[ix].intTerm;
                            final int targetGroup = rules[sortedRuleIndices[ix]].targetGroup;
                            remappings[targetGroup] = sortedInternalIndices[ix];
                        }
                    }
                    termIterator.close();
                } else {
                    final StringTermIterator termIterator = flamdexReader.getStringTermIterator(field);
                    String currentTerm = sortedConditions[fieldStartIndex].stringTerm;
                    int termStartIndex = fieldStartIndex;
                    final int fieldEndIndex = conditionIndex;
                    for (int ix = fieldStartIndex; ix <= fieldEndIndex; ix++) {
                        if (ix != fieldEndIndex) {
                            if (sortedConditions[ix].stringTerm.equals(currentTerm)) {
                                final int targetGroup = rules[sortedRuleIndices[ix]].targetGroup;
                                remappings[targetGroup] = sortedInternalIndices[ix];
                                continue;
                            }
                        }

                        performStringMultiEqualityRegroup(docIdToGroup, newDocIdToGroup, docIdBuf, docIdStream, termIterator, remappings, currentTerm, errorOnCollisions ? placeholderGroup : -1);

                        // Reset the remapping entries to placeholderGroup
                        for (int ix2 = termStartIndex; ix2 < ix; ix2++) {
                            final int targetGroup = rules[sortedRuleIndices[ix2]].targetGroup;
                            remappings[targetGroup] = placeholderGroup;
                        }

                        if (ix != fieldEndIndex) {
                            termStartIndex = ix;
                            currentTerm = sortedConditions[ix].stringTerm;
                            final int targetGroup = rules[sortedRuleIndices[ix]].targetGroup;
                            remappings[targetGroup] = sortedInternalIndices[ix];
                        }
                    }
                    termIterator.close();
                }
            }

            docIdStream.close();

            if (conditionIndex != numConditions) {
                // Identify next clump
                fieldStartIndex = conditionIndex;
                field = sortedConditions[conditionIndex].field;
                intType = sortedConditions[conditionIndex].intType;
                inequality = sortedConditions[conditionIndex].inequality;
            }
        }
    }

    private static void performStringMultiInequalityRegroup(GroupLookup docIdToGroup, GroupLookup newDocIdToGroup, int[] docIdBuf, DocIdStream docIdStream, int[] barrierLengths, String[][] barriers, int[][] resultingIndex, StringTermIterator termIterator) throws ImhotepOutOfMemoryException {
        while (termIterator.next()) {
            final String term = termIterator.term();
            docIdStream.reset(termIterator);
            // Memory claimed in regroup(GroupMultiRemapRule[]) (maxBarrierIndexBytes)
            final int[] currentBarrierIndices = new int[barriers.length];
            while (true) {
                final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                for (int i = 0; i < n; i++) {
                    final int docId = docIdBuf[i];
                    final int group = docIdToGroup.get(docId);
                    final String[] barriersForGroup = barriers[group];
                    if (barriersForGroup == null) continue;
                    while (currentBarrierIndices[group] < barrierLengths[group]
                           && term.compareTo(barriersForGroup[currentBarrierIndices[group]]) > 0) {
                        currentBarrierIndices[group]++;
                    }
                    if (currentBarrierIndices[group] == barrierLengths[group]) continue;
                    final int newInternalConditionIndex = resultingIndex[group][currentBarrierIndices[group]];
                    newDocIdToGroup.set(docId, Math.min(newDocIdToGroup.get(docId), newInternalConditionIndex));
                }
                if (n < docIdBuf.length) break;
            }
        }
    }

    private static void performIntMultiInequalityRegroup(GroupLookup docIdToGroup, GroupLookup newDocIdToGroup, int[] docIdBuf, DocIdStream docIdStream, int[] barrierLengths, long[][] barriers, int[][] resultingIndex, IntTermIterator termIterator) throws ImhotepOutOfMemoryException {
        while (termIterator.next()) {
            final long term = termIterator.term();
            docIdStream.reset(termIterator);
            // Memory claimed in regroup(GroupMultiRemapRule[]) (maxBarrierIndexBytes)
            final int[] currentBarrierIndices = new int[barriers.length];
            while (true) {
                final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                for (int i = 0; i < n; i++) {
                    final int docId = docIdBuf[i];
                    final int group = docIdToGroup.get(docId);
                    final long[] barriersForGroup = barriers[group];
                    if (barriersForGroup == null) continue;
                    while (currentBarrierIndices[group] < barrierLengths[group]
                            && term > barriersForGroup[currentBarrierIndices[group]]) {
                        currentBarrierIndices[group]++;
                    }
                    if (currentBarrierIndices[group] == barrierLengths[group]) continue;
                    final int newInternalConditionIndex = resultingIndex[group][currentBarrierIndices[group]];
                    newDocIdToGroup.set(docId, Math.min(newDocIdToGroup.get(docId), newInternalConditionIndex));
                }
                if (n < docIdBuf.length) break;
            }
        }
    }

    static void internalMultiRegroupCleanup(GroupLookup docIdToGroup, int numGroups, GroupMultiRemapRule[] rules, int highestTarget, GroupLookup newDocIdToGroup, int placeholderGroup) {
        // Memory claimed in regroup(GroupMultiRemapRule[])
        final GroupMultiRemapRule[] targetGroupToRule = new GroupMultiRemapRule[Math.max(highestTarget+1, numGroups)];
        for (GroupMultiRemapRule rule : rules) {
            targetGroupToRule[rule.targetGroup] = rule;
        }
        for (int i = 0; i < docIdToGroup.size(); i++) {
            final GroupMultiRemapRule rule = targetGroupToRule[docIdToGroup.get(i)];
            if (rule == null) continue;
            final int currentGroup = newDocIdToGroup.get(i);
            if (currentGroup == placeholderGroup) {
                docIdToGroup.set(i, rule.negativeGroup);
            } else {
                docIdToGroup.set(i, rule.positiveGroups[currentGroup]);
            }
        }
    }

    public static void moveUntargeted(GroupLookup docIdToGroup, int numGroups, GroupMultiRemapRule[] rules) {
        final FastBitSet moveToZero = new FastBitSet(numGroups);
        moveToZero.setAll();
        for (GroupMultiRemapRule rule : rules) {
            moveToZero.clear(rule.targetGroup);
        }
        for (int i = 0; i < docIdToGroup.size(); i++) {
            final int group = docIdToGroup.get(i);
            if (moveToZero.get(group)) {
                docIdToGroup.set(i, 0);
            }
        }
    }
}
