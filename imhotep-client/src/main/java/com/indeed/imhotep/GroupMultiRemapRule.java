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
 package com.indeed.imhotep;

import org.apache.commons.lang.StringEscapeUtils;

import java.util.Arrays;

public class GroupMultiRemapRule {
    public final int targetGroup;
    public final int negativeGroup;
    public final int[] positiveGroups;
    public final RegroupCondition[] conditions;

    public GroupMultiRemapRule(
            final int targetGroup,
            final int negativeGroup,
            final int[] positiveGroups,
            final RegroupCondition[] conditions) {
        if (conditions.length != positiveGroups.length) {
            throw new IllegalArgumentException("positiveGroups.length must equal conditions.length");
        }
        for (final RegroupCondition condition : conditions) {
            if (condition == null) {
                throw new IllegalArgumentException("cannot have null conditions");
            }
        }
        this.targetGroup = targetGroup;
        this.conditions = Arrays.copyOf(conditions, conditions.length);
        this.positiveGroups = Arrays.copyOf(positiveGroups, positiveGroups.length);
        this.negativeGroup = negativeGroup;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final GroupMultiRemapRule that = (GroupMultiRemapRule) o;

        return (negativeGroup == that.negativeGroup)
                && (targetGroup == that.targetGroup)
                && Arrays.equals(conditions, that.conditions)
                && Arrays.equals(positiveGroups, that.positiveGroups);
    }

    @Override
    public int hashCode() {
        int result = targetGroup;
        result = 31 * result + negativeGroup;
        result = 31 * result + (positiveGroups != null ? Arrays.hashCode(positiveGroups) : 0);
        result = 31 * result + (conditions != null ? Arrays.hashCode(conditions) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "GroupMultiRemapRule{" +
                "targetGroup=" + targetGroup +
                ", negativeGroup=" + negativeGroup +
                ", positiveGroups=" + Arrays.toString(positiveGroups) +
                ", conditions=" + Arrays.toString(conditions) +
                '}';
    }

    public String prettyPrint() {
        boolean oneTarget = true;
        for (final int group : positiveGroups) {
            if (negativeGroup != group) {
                oneTarget = false;
            }
        }

        if (oneTarget) {
            return "{" + targetGroup + " -> " + negativeGroup + "}";
        }

        final String field = conditions[0].field;
        boolean oneField = true;
        for (final RegroupCondition condition : conditions) {
            if (!condition.field.equals(field)) {
                oneField = false;
            }
        }
        final StringBuilder result = new StringBuilder();
        result.append("{")
              .append(targetGroup)
              .append(oneField ? " by " + field : "")
              .append(" -> [");
        for (int i = 0; i < conditions.length; i++) {
            final RegroupCondition condition = conditions[i];
            result.append(oneField ? "" : condition.field)
                  .append(condition.inequality ? "<=" : "")
                  .append(condition.intType ? condition.intTerm : ("\"" + StringEscapeUtils.escapeJava(condition.stringTerm) + "\""))
                  .append(" TO ")
                  .append(positiveGroups[i])
                  .append(", ");
        }
        result.append("DEFAULT TO ")
              .append(negativeGroup)
              .append("")
              .append("]}");
        return result.toString();
    }
}
