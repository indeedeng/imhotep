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
 package com.indeed.imhotep.marshal;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.indeed.flamdex.query.BooleanOp;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.TermCount;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.GroupRemapMessage;
import com.indeed.imhotep.protobuf.Operator;
import com.indeed.imhotep.protobuf.PerformanceStatsMessage;
import com.indeed.imhotep.protobuf.QueryMessage;
import com.indeed.imhotep.protobuf.QueryRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.imhotep.protobuf.TermCountMessage;
import com.indeed.imhotep.protobuf.TermMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author jsgroth
 */
public final class ImhotepDaemonMarshaller {
    private static final Map<Operator, BooleanOp> operatorMap = ImmutableMap.<Operator, BooleanOp>builder()
            .put(Operator.AND, BooleanOp.AND)
            .put(Operator.OR, BooleanOp.OR)
            .put(Operator.NOT, BooleanOp.NOT)
            .build();

    private ImhotepDaemonMarshaller() {}

    public static Term marshal(final TermMessage protoTerm) {
        return new Term(protoTerm.getFieldName(), protoTerm.getIsIntField(), protoTerm.getTermIntVal(), protoTerm.getTermStringVal());
    }

    public static Query marshal(final QueryMessage protoQuery) {
        if (protoQuery.hasOperator()) {
            final BooleanOp operator = operatorMap.get(protoQuery.getOperator());
            final List<Query> queryList = new ArrayList<>(protoQuery.getOperandCount());
            for (final QueryMessage query : protoQuery.getOperandList()) {
                queryList.add(marshal(query));
            }
            return Query.newBooleanQuery(operator, queryList);
        } else if (protoQuery.hasMaxTerm()) {
            return Query.newRangeQuery(
                    marshal(protoQuery.getMinTerm()),
                    marshal(protoQuery.getMaxTerm()),
                    protoQuery.getIsMaxInclusive()
            );
        } else {
            return Query.newTermQuery(marshal(protoQuery.getMinTerm()));
        }
    }

    public static QueryRemapRule marshal(final QueryRemapMessage protoRule) {
        return new QueryRemapRule(
                protoRule.getTargetGroup(),
                marshal(protoRule.getQuery()),
                protoRule.getNegativeGroup(),
                protoRule.getPositiveGroup()
        );
    }

    public static GroupRemapRule[] marshalGroupRemapMessageList(final List<GroupRemapMessage> protoRemapRules) {
        final GroupRemapRule[] ret = new GroupRemapRule[protoRemapRules.size()];
        for (int i = 0; i < protoRemapRules.size(); ++i) {
            final GroupRemapMessage protoRule = protoRemapRules.get(i);
            ret[i] = marshal(protoRule);
        }
        return ret;
    }

    public static GroupRemapRule marshal(final GroupRemapMessage protoRule) {
        final RegroupCondition condition = marshal(protoRule.getCondition());
        return new GroupRemapRule(protoRule.getTargetGroup(), condition, protoRule.getNegativeGroup(), protoRule.getPositiveGroup());
    }

    public static RegroupCondition marshal(final RegroupConditionMessage condition) {
        return new RegroupCondition(
                condition.getField(),
                condition.getIntType(),
                condition.hasIntTerm() ? condition.getIntTerm() : 0,
                condition.hasStringTerm() ? condition.getStringTerm() : "",
                condition.hasInequality() && condition.getInequality());
    }

    public static RegroupCondition[] marshalRegroupConditionMessageList(final List<RegroupConditionMessage> protoConditions) {
        final RegroupCondition[] ret = new RegroupCondition[protoConditions.size()];
        for (int i = 0; i < protoConditions.size(); i++) {
            final RegroupConditionMessage protoCondition = protoConditions.get(i);
            ret[i] = marshal(protoCondition);
        }
        return ret;
    }

    public static GroupMultiRemapRule marshal(final GroupMultiRemapMessage protoRule) {
        final int numSubRules = protoRule.getPositiveGroupCount();
        final RegroupCondition[] conditions = new RegroupCondition[numSubRules];
        final int[] positiveGroups = new int[numSubRules];
        for (int ix = 0; ix < numSubRules; ix++) {
            positiveGroups[ix] = protoRule.getPositiveGroup(ix);
            conditions[ix] = marshal(protoRule.getCondition(ix));
        }
        return new GroupMultiRemapRule(protoRule.getTargetGroup(), protoRule.getNegativeGroup(), positiveGroups, conditions);
    }

    public static GroupMultiRemapRule[] marshalGroupMultiRemapMessageList(final List<GroupMultiRemapMessage> protoRemapRules) {
        final GroupMultiRemapRule[] ret = new GroupMultiRemapRule[protoRemapRules.size()];
        for (int i = 0; i < protoRemapRules.size(); ++i) {
            final GroupMultiRemapMessage protoRule = protoRemapRules.get(i);
            ret[i] = marshal(protoRule);
        }
        return ret;
    }

    public static List<TermCountMessage> marshalTermCountList(final List<TermCount> termCountList) {
        final List<TermCountMessage> ret = Lists.newArrayListWithCapacity(termCountList.size());
        for (final TermCount termCount : termCountList) {
            ret.add(
                    TermCountMessage.newBuilder()
                    .setTerm(ImhotepClientMarshaller.marshal(termCount.getTerm()))
                    .setCount(termCount.getCount())
                    .build()
            );
        }
        return ret;
    }

    public static PerformanceStatsMessage marshal(final PerformanceStats stats) {
        return PerformanceStatsMessage.newBuilder()
                .setCpuTime(stats.cpuTime)
                .setMaxMemoryUsed(stats.maxMemoryUsage)
                .build();
    }
}
