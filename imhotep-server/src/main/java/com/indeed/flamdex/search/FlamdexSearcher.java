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
 package com.indeed.flamdex.search;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.datastruct.FastBitSetPooler;
import com.indeed.flamdex.datastruct.MockFastBitSetPooler;
import com.indeed.flamdex.query.BooleanOp;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.QueryType;
import com.indeed.flamdex.query.Term;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author jsgroth
 */
public class FlamdexSearcher {
    private final FlamdexReader r;

    public FlamdexSearcher(final FlamdexReader r) {
        this.r = r;
    }

    // for those who don't care about memory safety
    public FastBitSet search(final Query query) {
        final FastBitSet ret = new FastBitSet(r.getNumDocs());
        final QueryEvaluator evaluator = rewriteQuery(query);
        try {
            evaluator.or(r, ret, new MockFastBitSetPooler());
        } catch (final FlamdexOutOfMemoryException e) {
            throw new RuntimeException("wtf, DumbFastBitSetPooler doesn't actually throw this exception", e);
        }
        return ret;
    }

    public void search(final Query query, final FastBitSet bitSet, final FastBitSetPooler bitSetPooler) throws FlamdexOutOfMemoryException {
        final QueryEvaluator evaluator = rewriteQuery(query);
        evaluator.or(r, bitSet, bitSetPooler);
    }

    private static QueryEvaluator rewriteQuery(final Query query) {
        switch (query.getQueryType()) {
            case TERM:
                return new TermQueryEvaluator(query.getStartTerm());
            case BOOLEAN:
                if (query.getOperator() == BooleanOp.OR) {
                    return rewriteOr(query);
                } else {
                    final List<QueryEvaluator> operands = new ArrayList<>(query.getOperands().size());
                    for (final Query operand : query.getOperands()) {
                        operands.add(rewriteQuery(operand)); // oh no, recursion :(
                    }
                    return new BooleanQueryEvaluator(query.getOperator(), operands);
                }
            case RANGE:
                if (query.getStartTerm().isIntField()) {
                    return new IntRangeQueryEvaluator(query.getStartTerm(), query.getEndTerm(), query.isMaxInclusive());
                } else {
                    return new StringRangeQueryEvaluator(query.getStartTerm(), query.getEndTerm(), query.isMaxInclusive());
                }
            default:
                throw new IllegalArgumentException("unrecognized query type: " + query.getQueryType());
        }
    }

    /*
    Separate into two categories of operands
        - term queries
        - everything else
    For the term queries, group by field and build a IntTermSetQueryEvaluator or StringTermSetQueryEvaluator.
    For everything else, use whatever we typically use (call rewriteQuery)
     */
    private static QueryEvaluator rewriteOr(final Query query) {
        final List<QueryEvaluator> operands = new ArrayList<>(query.getOperands().size());
        final Map<String, List<Query>> stringFieldOperandMap = Maps.newHashMap();
        final Map<String, List<Query>> intFieldOperandMap = Maps.newHashMap();
        // Split out all the immediate terms that can be turned into an optimized OR query
        for (final Query operand : query.getOperands()) {
            if (operand.getQueryType() == QueryType.TERM) {
                final Term term = operand.getStartTerm();
                if (term.isIntField()) {
                    if (!intFieldOperandMap.containsKey(term.getFieldName())) {
                        intFieldOperandMap.put(term.getFieldName(), Lists.<Query>newArrayList());
                    }
                    intFieldOperandMap.get(term.getFieldName()).add(operand);
                } else {
                    if (!stringFieldOperandMap.containsKey(term.getFieldName())) {
                        stringFieldOperandMap.put(term.getFieldName(), Lists.<Query>newArrayList());
                    }
                    stringFieldOperandMap.get(term.getFieldName()).add(operand);
                }
            } else {
                // If they don't fit the bill, go ahead and do the unoptimized bit
                operands.add(rewriteQuery(operand));
            }
        }

        // Use those terms that were split out
        for (final Map.Entry<String, List<Query>> stringListEntry : stringFieldOperandMap.entrySet()) {
            final List<Query> queries = stringListEntry.getValue();
            final String[] terms = new String[queries.size()];
            for (int i=0;i<queries.size();i++) {
                terms[i] = queries.get(i).getStartTerm().getTermStringVal();
            }
            Arrays.sort(terms);
            operands.add(new StringTermSetQueryEvaluator(stringListEntry.getKey(), terms));
        }
        for (final Map.Entry<String, List<Query>> stringListEntry : intFieldOperandMap.entrySet()) {
            final List<Query> queries = stringListEntry.getValue();
            final long[] terms = new long[queries.size()];
            for (int i=0;i<queries.size();i++) {
                terms[i] = queries.get(i).getStartTerm().getTermIntVal();
            }
            Arrays.sort(terms);
            operands.add(new IntTermSetQueryEvaluator(stringListEntry.getKey(), terms));
        }

        return new BooleanQueryEvaluator(BooleanOp.OR, operands);
    }
}
