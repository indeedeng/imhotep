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
 package com.indeed.imhotep;

public class GroupRemapRule {
    public final int targetGroup;
    public final RegroupCondition condition;
    public final int negativeGroup;
    public final int positiveGroup;

    public GroupRemapRule(
            final int targetGroup,
            final RegroupCondition condition,
            final int negativeGroup,
            final int positiveGroup) {
        this.targetGroup = targetGroup;
        this.condition = condition;
        this.negativeGroup = negativeGroup;
        this.positiveGroup = positiveGroup;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final GroupRemapRule that = (GroupRemapRule) o;

        if (negativeGroup != that.negativeGroup) {
            return false;
        }
        if (positiveGroup != that.positiveGroup) {
            return false;
        }
        if (targetGroup != that.targetGroup) {
            return false;
        }
        if (condition != null ? !condition.equals(that.condition) : that.condition != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = targetGroup;
        result = 31 * result + (condition != null ? condition.hashCode() : 0);
        result = 31 * result + negativeGroup;
        result = 31 * result + positiveGroup;
        return result;
    }
}
