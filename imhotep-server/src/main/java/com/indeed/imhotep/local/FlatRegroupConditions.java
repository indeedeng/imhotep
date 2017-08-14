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
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.util.core.sort.Quicksortable;
import com.indeed.util.core.sort.Quicksortables;

final class FlatRegroupConditions {
    final GroupMultiRemapRule[] rules;
    final RegroupCondition[] conditions;
    final int[] internalIndices;
    final int[] positiveGroups;
    final int[] ruleIndices;

    FlatRegroupConditions(final GroupMultiRemapRule[] rules) {
        this.rules = rules;

        final int length = GroupMultiRemapRules.countRemapConditions(rules);
        conditions       = new RegroupCondition[length];
        internalIndices  = new int[length];
        positiveGroups   = new int[length];
        ruleIndices      = new int[length];

        int i = 0;
        for (int iRule = 0; iRule < rules.length; iRule++) {
            final GroupMultiRemapRule rule = rules[iRule];
            for (int iCondition = 0; iCondition < rule.conditions.length; iCondition++) {
                final RegroupCondition condition = rule.conditions[iCondition];
                conditions[i]      = condition;
                positiveGroups[i]  = rule.positiveGroups[iCondition];
                internalIndices[i] = iCondition;
                ruleIndices[i]     = iRule;
                i++;
            }
        }
        reorder();
    }

    int length() { return conditions.length; }

    boolean matches(final int     index,
                    final String  field,
                    final boolean intType,
                    final boolean inequality) {
        return
            conditions[index].field.equals(field)   &&
            conditions[index].intType    == intType &&
            conditions[index].inequality == inequality;
    }

    void reorder() {
        Quicksortables.sort(new Quicksortable() {
                @Override public void swap(final int i, final int j) {
                    Quicksortables.swap(conditions, i, j);
                    Quicksortables.swap(positiveGroups, i, j);
                    Quicksortables.swap(internalIndices, i, j);
                    Quicksortables.swap(ruleIndices, i, j);
                }

                @Override public int compare(final int i, final int j) {
                    return ComparisonChain.start()
                        .compare(conditions[i].field, conditions[j].field)
                        .compareFalseFirst(conditions[i].intType, conditions[j].intType)
                        .compareFalseFirst(conditions[i].inequality, conditions[j].inequality)
                        .compare(rules[ruleIndices[i]].targetGroup, rules[ruleIndices[j]].targetGroup)
                        .compare(internalIndices[i], internalIndices[j])
                        .result();
                }
            }, length());
    }

    void reorderOnTerm(final int start, final int end, final boolean intType) {
        Quicksortables.sort(new Quicksortable() {
                @Override public void swap(final int i, final int j) {
                    Quicksortables.swap(conditions, start + i, start + j);
                    Quicksortables.swap(positiveGroups, start + i, start + j);
                    Quicksortables.swap(internalIndices, start + i, start + j);
                    Quicksortables.swap(ruleIndices, start + i, start + j);
                }

                @Override public int compare(final int i, final int j) {
                    if (intType) {
                        return ComparisonChain.start()
                            .compare(conditions[start + i].intTerm, conditions[start + j].intTerm)
                            .result();
                    } else {
                        return ComparisonChain.start()
                            .compare(conditions[start + i].stringTerm, conditions[start + j].stringTerm)
                            .result();
                    }
                }
            }, end - start);
    }

    /*
     * Upon returning, barriers contains a sorted list of differentiation
     * points for each targetGroup in this inequality split.
     *
     * @param resultingIndex is a parallel array saying, for each entry in barriers, what
     * condition index it corresponds to.
     *
     * @param barrierLengths says to what extent the barriers for each group
     * are full (i.e., the effective length)
     */
    void formIntDividers(final int fieldStartIndex, final int conditionIndex,
                         final int[] barrierLengths, final long[][] barriers,
                         final int[][] resultingIndex) {
        for (int ix = fieldStartIndex; ix < conditionIndex; ix++) {
            final GroupMultiRemapRule rule         = rules[ruleIndices[ix]];
            final int                 targetGroup  = rule.targetGroup;
            final RegroupCondition    condition    = conditions[ix];
            final int                 currentIndex = barrierLengths[targetGroup];
            if (currentIndex == 0) {
                barriers[targetGroup] = new long[rule.conditions.length];
                resultingIndex[targetGroup] = new int[rule.conditions.length];
            }
            if (currentIndex == 0 ||
                condition.intTerm > barriers[targetGroup][currentIndex-1]) {
                barriers[targetGroup][currentIndex] = condition.intTerm;
                resultingIndex[targetGroup][currentIndex] = internalIndices[ix];
                barrierLengths[targetGroup]++;
            } else {
                throw new IllegalArgumentException("int inequality conditions that can never be met.");
            }
        }
    }

    /*
     * Upon returning, barriers contains a sorted list of differentiation
     * points for each targetGroup in this inequality split.
     *
     * @param resultingIndex is a parallel array saying, for each entry in
     * barriers, what condition index it corresponds to.
     *
     * @param barrierLengths says to what extent the barriers for each group
     * are full (i.e., the effective length)
     */
    void formStringDividers(final int fieldStartIndex, final int conditionIndex,
                            final int[] barrierLengths, final String[][] barriers,
                            final int[][] resultingIndex) {
        for (int ix = fieldStartIndex; ix < conditionIndex; ix++) {
            final GroupMultiRemapRule rule         = rules[ruleIndices[ix]];
            final int                 targetGroup  = rule.targetGroup;
            final RegroupCondition    condition    = conditions[ix];
            final int                 currentIndex = barrierLengths[targetGroup];
            if (currentIndex == 0) {
                barriers[targetGroup] = new String[rule.conditions.length];
                resultingIndex[targetGroup] = new int[rule.conditions.length];
            }
            if (currentIndex == 0 ||
                condition.stringTerm.compareTo(barriers[targetGroup][currentIndex-1]) > 0) {
                barriers[targetGroup][currentIndex] = condition.stringTerm;
                resultingIndex[targetGroup][currentIndex] = internalIndices[ix];
                barrierLengths[targetGroup]++;
            } else {
                throw new IllegalArgumentException("String inequality conditions that can never be met.");
            }
        }
    }
}

