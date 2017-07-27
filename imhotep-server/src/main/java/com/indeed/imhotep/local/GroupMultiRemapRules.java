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

import java.util.Arrays;

class GroupMultiRemapRules {
    private GroupMultiRemapRules() {
    }

    static int countRemapConditions(final GroupMultiRemapRule[] rules) {
        int result = 0;
        for (final GroupMultiRemapRule rule : rules) {
            result += rule.conditions.length;
        }
        return result;
    }

    static int findMaxGroup(final GroupMultiRemapRule[] rules) {
        int maxGroup = 0;
        for (final GroupMultiRemapRule rule : rules) {
            for (final int positiveGroup : rule.positiveGroups) {
                maxGroup = Math.max(maxGroup, positiveGroup);
            }
            maxGroup = Math.max(maxGroup, rule.targetGroup);
            maxGroup = Math.max(maxGroup, rule.negativeGroup);
        }
        return maxGroup;
    }

    static int findMaxIntermediateGroup(final GroupMultiRemapRule[] rules) {
        int max = 0;
        for (final GroupMultiRemapRule rule : rules) {
            max = Math.max(max, rule.conditions.length);
        }
        return max;
    }

    /*
     * Verifies that targetGroups are unique and are >= 1, and returns the highest group found.
     */
    static int validateTargets(final GroupMultiRemapRule[] rules) {
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
    static void validateEqualitySplits(final GroupMultiRemapRule[] rules) {
        for (final GroupMultiRemapRule rule : rules) {
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
                            throw new IllegalArgumentException("Duplicate equality split term " + s1.intTerm +
                                                               " in int field " + s1.field);
                        }
                    } else {
                        if (s1.stringTerm.equals(s2.stringTerm)) {
                            throw new IllegalArgumentException("Duplicate equality split term \"" + s1.stringTerm +
                                                               "\" in string field " + s1.field);
                        }
                    }
                }
            }
        }
    }

    private static void sortConditions(final RegroupCondition[] sortedConditions) {
        Quicksortables.sort(new Quicksortable() {
                @Override
                public void swap(final int i, final int j) {
                    final RegroupCondition tmp = sortedConditions[i];
                    sortedConditions[i] = sortedConditions[j];
                    sortedConditions[j] = tmp;
                }

                @Override
                public int compare(final int i, final int j) {
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
}
