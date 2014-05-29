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

    public QueryRemapRule(int targetGroup, Query query, int negativeGroup, int positiveGroup) {
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
