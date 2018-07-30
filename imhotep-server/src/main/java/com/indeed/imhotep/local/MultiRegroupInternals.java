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
package com.indeed.imhotep.local;

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.utils.FlamdexUtils;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.exceptions.MultiValuedFieldRegroupException;

import java.io.IOException;
import java.util.Arrays;

class MultiRegroupInternals {
    private MultiRegroupInternals() {
    }

    private static void remapDocsInTargetGroups(final GroupLookup docIdToGroup,
                                                final GroupLookup newLookup,
                                                final int[] docIdBuf,
                                                final DocIdStream docIdStream,
                                                final int[] remappings,
                                                final int placeHolderGroup) {
        while (true) {
            final int n = docIdStream.fillDocIdBuffer(docIdBuf);
            for (int i = 0; i < n; i++) {
                final int docId = docIdBuf[i];
                final int oldGroup = docIdToGroup.get(docId);
                if (oldGroup != 0) {
                    final int currentGroup = newLookup.get(docId);
                    if (placeHolderGroup > 0) {
                        if (currentGroup != placeHolderGroup) {
                            final String message =
                                "Regrouping on a multi-valued field " +
                                "doesn't work correctly so the operation is rejected.";
                            throw new MultiValuedFieldRegroupException(message);
                        }
                    }
                    newLookup.set(docId, Math.min(currentGroup, remappings[oldGroup]));
                }
            }
            if (n < docIdBuf.length) {
                break;
            }
        }
    }

    static void performStringMultiEqualityRegroup(
            final GroupLookup docIdToGroup,
            final GroupLookup newLookup,
            final int[] docIdBuf,
            final DocIdStream docIdStream,
            final StringTermIterator termIterator,
            final int[] remappings,
            final String term,
            final int placeHolderGroup)
        throws IOException {
        termIterator.reset(term);
        if (termIterator.next() && termIterator.term().equals(term)) {
            docIdStream.reset(termIterator);
            remapDocsInTargetGroups(docIdToGroup, newLookup,
                                    docIdBuf, docIdStream,
                                    remappings, placeHolderGroup);
        }
    }

    static void performIntMultiEqualityRegroup(
            final GroupLookup docIdToGroup,
            final GroupLookup newLookup,
            final int[] docIdBuf,
            final DocIdStream docIdStream,
            final IntTermIterator termIterator,
            final int[] remappings,
            final long term,
            final int placeHolderGroup)
        throws IOException {
        termIterator.reset(term);
        if (termIterator.next() && termIterator.term() == term) {
            docIdStream.reset(termIterator);
            remapDocsInTargetGroups(docIdToGroup, newLookup,
                                    docIdBuf, docIdStream,
                                    remappings, placeHolderGroup);
        }
    }

    static void internalMultiRegroup(final GroupLookup docIdToGroup,
                                     final GroupLookup newDocIdToGroup,
                                     final int[] docIdBuf,
                                     final FlamdexReader flamdexReader,
                                     final GroupMultiRemapRule[] rules,
                                     final int highestTarget,
                                     final int numConditions,
                                     final int placeholderGroup,
                                     final int maxGroup,
                                     final boolean errorOnCollisions)
        throws IOException, ImhotepOutOfMemoryException {

        // Make a bunch of parallel arrays so we can sort. Memory claimed in parallelArrayBytes.
        final FlatRegroupConditions flat = new FlatRegroupConditions(rules);
        if (flat.conditions.length == 0) {
            // there is no rules with conditions, quit.
            // all regrouping will be in internalMultiRegroupCleanup method.
            return;
        }

        // memory claimed in remappingBytes
        final int[] remappings = new int[maxGroup + 1];
        Arrays.fill(remappings, placeholderGroup);
        remappings[0] = 0;

        int fieldStartIndex = 0;
        String  field      = flat.conditions[0].field;
        boolean intType    = flat.conditions[0].intType;
        boolean inequality = flat.conditions[0].inequality;
        for (int iCondition = 1; iCondition <= numConditions; iCondition++) {
            if ((iCondition != numConditions) &&
                flat.matches(iCondition, field, intType, inequality)) {
                continue;
            }

            // End of field/field type clump; perform regroups.
            try (final DocIdStream docIdStream = flamdexReader.getDocIdStream()) {
                if (inequality) {
                    // These two branches both rely on the fact that these parallel arrays
                    // are sorted within this subarray by condition index within their rule.

                    // Handle inequalities by finding, for each document, which range it falls
                    // in to, and reassigning if it is an earlier rule than the one it presently
                    // matches (or if it presently matches none at all).

                    // Memory for these claimed earlier (see maxInequalityBytes)
                    final int[] barrierLengths = new int[highestTarget+1];
                    final int[][] resultingIndex = new int[highestTarget+1][];
                    if (intType) {
                        final long[][] barriers = new long[highestTarget+1][];
                        flat.formIntDividers(fieldStartIndex, iCondition,
                                barrierLengths, barriers, resultingIndex);

                        try (final IntTermIterator termIterator = flamdexReader.getIntTermIterator(field)) {
                            performIntMultiInequalityRegroup(docIdToGroup, newDocIdToGroup, docIdBuf, docIdStream,
                                    barrierLengths, barriers, resultingIndex, termIterator);
                        }
                    } else {
                        final String[][] barriers = new String[highestTarget+1][];
                        flat.formStringDividers(fieldStartIndex, iCondition,
                                barrierLengths, barriers, resultingIndex);

                        try (final StringTermIterator termIterator = flamdexReader.getStringTermIterator(field)) {
                            performStringMultiInequalityRegroup(docIdToGroup, newDocIdToGroup, docIdBuf, docIdStream,
                                    barrierLengths, barriers, resultingIndex, termIterator);
                        }
                    }
                } else {
                    // Handle term splits by going to the term directly and applying the rule.
                    flat.reorderOnTerm(fieldStartIndex, iCondition, intType);
                    if (intType) {
                        try (final IntTermIterator termIterator = flamdexReader.getUnsortedIntTermIterator(field)) {
                            // If we sure that there are no multi terms for docs,
                            // then errorOnCollision value is unimportant
                            final boolean errorOnCollisionsExt = errorOnCollisions &&
                                    (FlamdexUtils.hasMultipleTermDoc(flamdexReader, field, true) != Boolean.FALSE);
                            long currentTerm = flat.conditions[fieldStartIndex].intTerm;
                            int termStartIndex = fieldStartIndex;
                            final int fieldEndIndex = iCondition;
                            for (int ix = fieldStartIndex; ix <= fieldEndIndex; ix++) {
                                if (ix != fieldEndIndex) {
                                    if (flat.conditions[ix].intTerm == currentTerm) {
                                        final int targetGroup = rules[flat.ruleIndices[ix]].targetGroup;
                                        remappings[targetGroup] = flat.internalIndices[ix];
                                        continue;
                                    }
                                }

                                performIntMultiEqualityRegroup(docIdToGroup, newDocIdToGroup,
                                        docIdBuf, docIdStream, termIterator,
                                        remappings, currentTerm,
                                        errorOnCollisionsExt ? placeholderGroup : -1);

                                for (int ix2 = termStartIndex; ix2 < ix; ix2++) {
                                    final int targetGroup = rules[flat.ruleIndices[ix2]].targetGroup;
                                    remappings[targetGroup] = placeholderGroup;
                                }

                                if (ix != fieldEndIndex) {
                                    termStartIndex = ix;
                                    currentTerm = flat.conditions[ix].intTerm;
                                    final int targetGroup = rules[flat.ruleIndices[ix]].targetGroup;
                                    remappings[targetGroup] = flat.internalIndices[ix];
                                }
                            }
                        }
                    } else {
                        try (final StringTermIterator termIterator = flamdexReader.getStringTermIterator(field)) {
                            // If we sure that there are no multi terms for docs,
                            // then errorOnCollision value is unimportant
                            final boolean errorOnCollisionsExt = errorOnCollisions &&
                                    (FlamdexUtils.hasMultipleTermDoc(flamdexReader, field, false) != Boolean.FALSE);
                            String currentTerm = flat.conditions[fieldStartIndex].stringTerm;
                            int termStartIndex = fieldStartIndex;
                            final int fieldEndIndex = iCondition;
                            for (int ix = fieldStartIndex; ix <= fieldEndIndex; ix++) {
                                if (ix != fieldEndIndex) {
                                    if (flat.conditions[ix].stringTerm.equals(currentTerm)) {
                                        final int targetGroup = rules[flat.ruleIndices[ix]].targetGroup;
                                        remappings[targetGroup] = flat.internalIndices[ix];
                                        continue;
                                    }
                                }

                                performStringMultiEqualityRegroup(docIdToGroup, newDocIdToGroup,
                                        docIdBuf, docIdStream, termIterator,
                                        remappings, currentTerm,
                                        errorOnCollisionsExt ? placeholderGroup : -1);

                                // Reset the remapping entries to placeholderGroup
                                for (int ix2 = termStartIndex; ix2 < ix; ix2++) {
                                    final int targetGroup = rules[flat.ruleIndices[ix2]].targetGroup;
                                    remappings[targetGroup] = placeholderGroup;
                                }

                                if (ix != fieldEndIndex) {
                                    termStartIndex = ix;
                                    currentTerm = flat.conditions[ix].stringTerm;
                                    final int targetGroup = rules[flat.ruleIndices[ix]].targetGroup;
                                    remappings[targetGroup] = flat.internalIndices[ix];
                                }
                            }
                        }
                    }
                }
            }

            if (iCondition != numConditions) {
                // Identify next clump
                fieldStartIndex = iCondition;
                field = flat.conditions[iCondition].field;
                intType = flat.conditions[iCondition].intType;
                inequality = flat.conditions[iCondition].inequality;
            }
        }
    }

    private static void performStringMultiInequalityRegroup(
            final GroupLookup docIdToGroup,
            final GroupLookup newDocIdToGroup,
            final int[] docIdBuf,
            final DocIdStream docIdStream,
            final int[] barrierLengths,
            final String[][] barriers,
            final int[][] resultingIndex,
            final StringTermIterator termIterator)
        throws ImhotepOutOfMemoryException {
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
                    if (barriersForGroup == null) {
                        continue;
                    }
                    while (currentBarrierIndices[group] < barrierLengths[group]
                           && term.compareTo(barriersForGroup[currentBarrierIndices[group]]) > 0) {
                        currentBarrierIndices[group]++;
                    }
                    if (currentBarrierIndices[group] == barrierLengths[group]) {
                        continue;
                    }
                    final int newInternalConditionIndex = resultingIndex[group][currentBarrierIndices[group]];
                    newDocIdToGroup.set(docId, Math.min(newDocIdToGroup.get(docId), newInternalConditionIndex));
                }
                if (n < docIdBuf.length) {
                    break;
                }
            }
        }
    }

    private static void performIntMultiInequalityRegroup(
            final GroupLookup docIdToGroup,
            final GroupLookup newDocIdToGroup,
            final int[] docIdBuf,
            final DocIdStream docIdStream,
            final int[] barrierLengths,
            final long[][] barriers,
            final int[][] resultingIndex,
            final IntTermIterator termIterator)
        throws ImhotepOutOfMemoryException {
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
                    if (barriersForGroup == null) {
                        continue;
                    }
                    while (currentBarrierIndices[group] < barrierLengths[group]
                            && term > barriersForGroup[currentBarrierIndices[group]]) {
                        currentBarrierIndices[group]++;
                    }
                    if (currentBarrierIndices[group] == barrierLengths[group]) {
                        continue;
                    }
                    final int newInternalConditionIndex = resultingIndex[group][currentBarrierIndices[group]];
                    newDocIdToGroup.set(docId, Math.min(newDocIdToGroup.get(docId), newInternalConditionIndex));
                }
                if (n < docIdBuf.length) {
                    break;
                }
            }
        }
    }

    static void internalMultiRegroupCleanup(final GroupLookup docIdToGroup,
                                            final int numGroups,
                                            final GroupMultiRemapRule[] rules,
                                            final int highestTarget,
                                            final GroupLookup newDocIdToGroup,
                                            final int placeholderGroup) {
        // Memory claimed in regroup(GroupMultiRemapRule[])
        final GroupMultiRemapRule[] targetGroupToRule =
            new GroupMultiRemapRule[Math.max(highestTarget+1, numGroups)];
        for (final GroupMultiRemapRule rule : rules) {
            targetGroupToRule[rule.targetGroup] = rule;
        }
        for (int i = 0; i < docIdToGroup.size(); i++) {
            final GroupMultiRemapRule rule = targetGroupToRule[docIdToGroup.get(i)];
            if (rule == null) {
                continue;
            }
            final int currentGroup = newDocIdToGroup.get(i);
            if (currentGroup == placeholderGroup) {
                docIdToGroup.set(i, rule.negativeGroup);
            } else {
                docIdToGroup.set(i, rule.positiveGroups[currentGroup]);
            }
        }
    }

    public static void moveUntargeted(final GroupLookup docIdToGroup,
                                      final int numGroups,
                                      final GroupMultiRemapRule[] rules) {
        final FastBitSet moveToZero = new FastBitSet(numGroups);
        moveToZero.setAll();
        for (final GroupMultiRemapRule rule : rules) {
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
