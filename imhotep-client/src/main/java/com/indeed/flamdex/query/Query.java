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
 package com.indeed.flamdex.query;

import com.google.common.base.Joiner;

import java.util.Collections;
import java.util.List;

/**
 * @author jsgroth
 */
public class Query {
    private final BooleanOp operator;
    private final List<Query> operands;

    private final Term startTerm;
    private final Term endTerm;
    private final boolean isMaxInclusive;

    private Query(final BooleanOp operator, final List<Query> operands) {
        this.operator = operator;
        this.operands = operands;
        startTerm = null;
        endTerm = null;
        isMaxInclusive = false;
    }

    private Query(final Term startTerm) {
        operator = null;
        operands = null;
        this.startTerm = startTerm;
        endTerm = null;
        isMaxInclusive = false;
    }

    private Query(final Term startTerm, final Term endTerm, final boolean maxInclusive) {
        operator = null;
        operands = null;
        this.startTerm = startTerm;
        this.endTerm = endTerm;
        isMaxInclusive = maxInclusive;
    }

    public static Query newBooleanQuery(final BooleanOp operator, final List<Query> operands) {
        if (operator == null || operands == null || operands.isEmpty()) {
            throw new IllegalArgumentException("invalid arguments: operator=" + operator + ", operands = "+operands);
        }
        if (operator == BooleanOp.NOT && operands.size() > 1) {
            return new Query(BooleanOp.NOT, Collections.singletonList(newBooleanQuery(BooleanOp.OR, operands)));
        }
        return new Query(operator, operands);
    }

    public static Query newTermQuery(final Term term) {
        if (term == null) {
            throw new IllegalArgumentException("term cannot be null");
        }
        return new Query(term);
    }

    public static Query newRangeQuery(final String field, final long startTerm, final long endTerm, final boolean isMaxInclusive) {
        return newRangeQuery(new Term(field, true, startTerm, ""), new Term(field, true, endTerm, ""), isMaxInclusive);
    }

    public static Query newRangeQuery(final String field, final String startTerm, final String endTerm, final boolean isMaxInclusive) {
        return newRangeQuery(new Term(field, false, 0, startTerm), new Term(field, false, 0, endTerm), isMaxInclusive);
    }

    public static Query newRangeQuery(final Term startTerm, final Term endTerm, final boolean isMaxInclusive) {
        if (startTerm == null || endTerm == null) {
            throw new IllegalArgumentException("term arguments cannot be null");
        }
        if (!startTerm.getFieldName().equals(endTerm.getFieldName())) {
            throw new IllegalArgumentException("field names do not match");
        }
        if (startTerm.isIntField() != endTerm.isIntField()) {
            throw new IllegalArgumentException("terms do not have the same field type");
        }
        if (endTerm.getTermIntVal() < startTerm.getTermIntVal()) {
            throw new IllegalArgumentException("end term must be >= start term");
        }
        return new Query(startTerm, endTerm, isMaxInclusive);
    }

    public QueryType getQueryType() {
        if (operator != null) {
            return QueryType.BOOLEAN;
        }
        if (endTerm != null) {
            return QueryType.RANGE;
        }
        return QueryType.TERM;
    }

    public BooleanOp getOperator() {
        return operator;
    }

    public List<Query> getOperands() {
        return operands;
    }

    public Term getStartTerm() {
        return startTerm;
    }

    public Term getEndTerm() {
        return endTerm;
    }

    public boolean isMaxInclusive() {
        return isMaxInclusive;
    }

    @Override
    public String toString() {
        switch (getQueryType()) {
            case TERM:
                return startTerm.toString();
            case BOOLEAN:
                if (operator == BooleanOp.NOT) {
                    return "NOT (" + Joiner.on(", ").join(operands) + ")";
                } else {
                    return "(" + Joiner.on(" " + operator.toString() + " ").join(operands) + ")";
                }
            case RANGE:
                return (startTerm.isIntField() ? "int:" : "str:") + startTerm.getFieldName() + ":[" +
                        (startTerm.isIntField() ? startTerm.getTermIntVal() : startTerm.getTermStringVal()) + " TO " +
                        (endTerm.isIntField() ? endTerm.getTermIntVal() : endTerm.getTermStringVal()) + (isMaxInclusive ? "]" : ")");
            default:
                return super.toString();
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof Query)) {
            return false;
        }

        final Query other = (Query) o;

        final QueryType queryType = getQueryType();
        if (other.getQueryType() != queryType) {
            return false;
        }

        switch (queryType) {
            case TERM:
                return startTerm.equals(other.startTerm);
            case BOOLEAN:
                return operator == other.operator && operands.equals(other.operands);
            case RANGE:
                return startTerm.equals(other.startTerm) && endTerm.equals(other.endTerm)
                        && isMaxInclusive == other.isMaxInclusive;
            default:
                throw new AssertionError("can't happen");
        }
    }

    @Override
    public int hashCode() {
        final QueryType queryType = getQueryType();
        int hashCode = queryType.hashCode();

        switch (queryType) {
            case TERM:
                hashCode *= 31;
                hashCode += startTerm.hashCode();
                break;

            case BOOLEAN:
                hashCode *= 31;
                hashCode += operator.hashCode();
                hashCode *= 31;
                hashCode += operands.hashCode();
                break;

            case RANGE:
                hashCode *= 31;
                hashCode += startTerm.hashCode();
                hashCode *= 31;
                hashCode += endTerm.hashCode();
                hashCode *= 31;
                hashCode += isMaxInclusive ? 1231 : 1237;
                break;

            default:
                throw new AssertionError("can't happen");
        }

        return hashCode;
    }
}
