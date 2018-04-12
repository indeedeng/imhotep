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

import com.indeed.flamdex.query.Query;

/**
 * @author jsgroth
 */
public class QueryRemapRule {
    private final int targetGroup;
    private final Query query;
    private final int negativeGroup;
    private final int positiveGroup;

    public QueryRemapRule(final int targetGroup, final Query query, final int negativeGroup, final int positiveGroup) {
        this.targetGroup = targetGroup;
        this.query = query;
        this.negativeGroup = negativeGroup;
        this.positiveGroup = positiveGroup;
    }

    public int getTargetGroup() {
        return targetGroup;
    }

    public Query getQuery() {
        return query;
    }

    public int getNegativeGroup() {
        return negativeGroup;
    }

    public int getPositiveGroup() {
        return positiveGroup;
    }

    public String toString() {
        return "QueryRemapRule{" +
                "targetGroup=" + targetGroup +
                ", query=" + query +
                ", negativeGroup=" + negativeGroup +
                ", positiveGroup=" + positiveGroup +
                '}';
    }
}
