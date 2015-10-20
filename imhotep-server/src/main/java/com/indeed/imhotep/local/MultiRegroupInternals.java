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

class MultiRegroupInternals {

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
                            final String message =
                                "Regrouping on a multi-valued field " +
                                "doesn't work correctly so the operation is rejected.";
                            throw new IllegalArgumentException(message);
                        }
                    }
                    newLookup.set(docId, Math.min(currentGroup, remappings[oldGroup]));
                }
            }
            if (n < docIdBuf.length) break;
        }
    }

    static void performStringMultiEqualityRegroup(GroupLookup docIdToGroup, GroupLookup newLookup,
                                                  int[] docIdBuf, DocIdStream docIdStream,
                                                  StringTermIterator termIterator,
                                                  int[] remappings, String term, int placeHolderGroup)
        throws IOException {
        termIterator.reset(term);
        if (termIterator.next() && termIterator.term().equals(term)) {
            if (docIdToGroup instanceof MultiCache.MultiCacheGroupLookup &&
                newLookup instanceof ArrayBasedGroupLookup &&
                termIterator instanceof SimpleStringTermIterator) {
                final long nativeShardDataPtr =
                    ((MultiCache.MultiCacheGroupLookup) docIdToGroup).nativeShardDataPtr();
                final SimpleStringTermIterator ssTermIterator = (SimpleStringTermIterator) termIterator;
                final long docListAddress = ssTermIterator.getDocListAddress() + ssTermIterator.getOffset();
                final int  nDocs          = ssTermIterator.docFreq();
                if (newLookup instanceof ByteGroupLookup) {
                    nativeRemapDocsInTargetGroups(nativeShardDataPtr,
                                                  ((ByteGroupLookup) newLookup).getDocIdToGroup(),
                                                  docListAddress, nDocs, remappings, placeHolderGroup);
                }
                else if (newLookup instanceof CharGroupLookup) {
                    nativeRemapDocsInTargetGroups(nativeShardDataPtr,
                                                  ((CharGroupLookup) newLookup).getDocIdToGroup(),
                                                  docListAddress, nDocs, remappings, placeHolderGroup);
                }
                else if (newLookup instanceof IntGroupLookup) {
                    nativeRemapDocsInTargetGroups(nativeShardDataPtr,
                                                  ((IntGroupLookup) newLookup).getDocIdToGroup(),
                                                  docListAddress, nDocs, remappings, placeHolderGroup);
                }
                else {
                    throw new UnsupportedOperationException("software error: unsupported GroupLookup type!");
                }
            } else {
                docIdStream.reset(termIterator);
                remapDocsInTargetGroups(docIdToGroup, newLookup,
                                        docIdBuf, docIdStream,
                                        remappings, placeHolderGroup);
            }
        }
    }

    static void performIntMultiEqualityRegroup(GroupLookup docIdToGroup, GroupLookup newLookup,
                                               int[] docIdBuf, DocIdStream docIdStream,
                                               IntTermIterator termIterator,
                                               int[] remappings, long term, int placeHolderGroup)
        throws IOException {
        termIterator.reset(term);
        if (termIterator.next() && termIterator.term() == term) {
            if (docIdToGroup instanceof MultiCache.MultiCacheGroupLookup &&
                newLookup instanceof ArrayBasedGroupLookup &&
                termIterator instanceof SimpleIntTermIterator) {
                final long nativeShardDataPtr =
                    ((MultiCache.MultiCacheGroupLookup) docIdToGroup).nativeShardDataPtr();
                final SimpleIntTermIterator ssTermIterator = (SimpleIntTermIterator) termIterator;
                final long docListAddress = ssTermIterator.getDocListAddress() + ssTermIterator.getOffset();
                final int  nDocs          = ssTermIterator.docFreq();
                if (newLookup instanceof ByteGroupLookup) {
                    nativeRemapDocsInTargetGroups(nativeShardDataPtr,
                                                  ((ByteGroupLookup) newLookup).getDocIdToGroup(),
                                                  docListAddress, nDocs, remappings, placeHolderGroup);
                }
                else if (newLookup instanceof CharGroupLookup) {
                    nativeRemapDocsInTargetGroups(nativeShardDataPtr,
                                                  ((CharGroupLookup) newLookup).getDocIdToGroup(),
                                                  docListAddress, nDocs, remappings, placeHolderGroup);
                }
                else if (newLookup instanceof IntGroupLookup) {
                    nativeRemapDocsInTargetGroups(nativeShardDataPtr,
                                                  ((IntGroupLookup) newLookup).getDocIdToGroup(),
                                                  docListAddress, nDocs, remappings, placeHolderGroup);
                }
                else {
                    throw new UnsupportedOperationException("software error: unsupported GroupLookup type!");
                }
            } else {
                docIdStream.reset(termIterator);
                remapDocsInTargetGroups(docIdToGroup, newLookup,
                                        docIdBuf, docIdStream,
                                        remappings, placeHolderGroup);
            }
        }
    }

    static void internalMultiRegroup(GroupLookup docIdToGroup,
                                     GroupLookup newDocIdToGroup,
                                     int[] docIdBuf,
                                     FlamdexReader flamdexReader,
                                     GroupMultiRemapRule[] rules,
                                     int highestTarget,
                                     int numConditions,
                                     int placeholderGroup,
                                     int maxGroup,
                                     boolean errorOnCollisions)
        throws IOException, ImhotepOutOfMemoryException {

        // Make a bunch of parallel arrays so we can sort. Memory claimed in parallelArrayBytes.
        FlatRegroupConditions flat = new FlatRegroupConditions(rules);

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
            final DocIdStream docIdStream = flamdexReader.getDocIdStream();
            if (inequality) {
                // These two branches both rely on the fact that these parallel arrays
                // are sorted within this subarray by condition index within their rule.

                // Handle inequalities by finding, for each document, which range it falls
                // in to, and reassigning if it is an earlier rule than the one it presently
                // matches (or if it presently matches none at all).

                // Memory for these claimed earlier (see maxInequalityBytes)
                final int[]   barrierLengths = new int[highestTarget+1];
                final int[][] resultingIndex = new int[highestTarget+1][];
                if (intType) {
                    final long[][] barriers = new long[highestTarget+1][];
                    flat.formIntDividers(fieldStartIndex, iCondition,
                                           barrierLengths, barriers, resultingIndex);

                    final IntTermIterator termIterator = flamdexReader.getIntTermIterator(field);
                    performIntMultiInequalityRegroup(docIdToGroup, newDocIdToGroup, docIdBuf, docIdStream,
                                                     barrierLengths, barriers, resultingIndex, termIterator);
                    termIterator.close();
                } else {
                    final String[][] barriers = new String[highestTarget+1][];
                    flat.formStringDividers(fieldStartIndex, iCondition,
                                              barrierLengths, barriers, resultingIndex);

                    final StringTermIterator termIterator = flamdexReader.getStringTermIterator(field);
                    performStringMultiInequalityRegroup(docIdToGroup, newDocIdToGroup, docIdBuf, docIdStream,
                                                        barrierLengths, barriers, resultingIndex, termIterator);
                    termIterator.close();
                }
            } else {
                // Handle term splits by going to the term directly and applying the rule.
                flat.reorderOnTerm(fieldStartIndex, iCondition, intType);
                if (intType) {
                    final IntTermIterator termIterator = flamdexReader.getUnsortedIntTermIterator(field);
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
                                                       errorOnCollisions ? placeholderGroup: -1);

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
                    termIterator.close();
                } else {
                    final StringTermIterator termIterator = flamdexReader.getStringTermIterator(field);
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
                                                          errorOnCollisions ? placeholderGroup : -1);

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
                    termIterator.close();
                }
            }

            docIdStream.close();

            if (iCondition != numConditions) {
                // Identify next clump
                fieldStartIndex = iCondition;
                field = flat.conditions[iCondition].field;
                intType = flat.conditions[iCondition].intType;
                inequality = flat.conditions[iCondition].inequality;
            }
        }
    }

    private static void performStringMultiInequalityRegroup(GroupLookup docIdToGroup, GroupLookup newDocIdToGroup,
                                                            int[] docIdBuf, DocIdStream docIdStream,
                                                            int[] barrierLengths, String[][] barriers,
                                                            int[][] resultingIndex,
                                                            StringTermIterator termIterator)
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

    private static void performIntMultiInequalityRegroup(GroupLookup docIdToGroup, GroupLookup newDocIdToGroup,
                                                         int[] docIdBuf, DocIdStream docIdStream,
                                                         int[] barrierLengths, long[][] barriers,
                                                         int[][] resultingIndex,
                                                         IntTermIterator termIterator)
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

    static void internalMultiRegroupCleanup(GroupLookup docIdToGroup,
                                            int numGroups,
                                            GroupMultiRemapRule[] rules,
                                            int highestTarget,
                                            GroupLookup newDocIdToGroup,
                                            int placeholderGroup) {
        // Memory claimed in regroup(GroupMultiRemapRule[])
        final GroupMultiRemapRule[] targetGroupToRule =
            new GroupMultiRemapRule[Math.max(highestTarget+1, numGroups)];
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

    public static void moveUntargeted(GroupLookup docIdToGroup,
                                      int numGroups,
                                      GroupMultiRemapRule[] rules) {
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

    private native static void nativeRemapDocsInTargetGroups(long nativeShardDataPtr,
                                                             byte[] results,
                                                             long docListAddress,
                                                             int nDocs,
                                                             int[] remappings,
                                                             int placeHolderGroup);

    private native static void nativeRemapDocsInTargetGroups(long nativeShardDataPtr,
                                                             char[] results,
                                                             long docListAddress,
                                                             int nDocs,
                                                             int[] remappings,
                                                             int placeHolderGroup);

    private native static void nativeRemapDocsInTargetGroups(long nativeShardDataPtr,
                                                             int[] results,
                                                             long docListAddress,
                                                             int nDocs,
                                                             int[] remappings,
                                                             int placeHolderGroup);
}
