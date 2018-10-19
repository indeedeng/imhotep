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
import com.indeed.imhotep.protobuf.StringLongMessage;
import com.indeed.imhotep.protobuf.TermCountMessage;
import com.indeed.imhotep.protobuf.TermMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jsgroth
 */
public final class ImhotepClientMarshaller {
    private static final Map<BooleanOp, Operator> operatorMap = ImmutableMap.<BooleanOp, Operator>builder()
            .put(BooleanOp.AND, Operator.AND)
            .put(BooleanOp.OR, Operator.OR)
            .put(BooleanOp.NOT, Operator.NOT)
            .build();

    private ImhotepClientMarshaller() {}

    public static RegroupConditionMessage marshal(final RegroupCondition condition) {
        final RegroupConditionMessage.Builder builder = RegroupConditionMessage.newBuilder()
                .setField(condition.field)
                .setIntType(condition.intType);
        if (condition.intType) {
            builder.setIntTerm(condition.intTerm);
        } else {
            builder.setStringTerm(condition.stringTerm);
        }
        if (condition.inequality) {
            builder.setInequality(true);
        }
        return builder.build();
    }

    public static List<GroupRemapMessage> marshal(final GroupRemapRule[] remapRules) {
        final List<GroupRemapMessage> ret = new ArrayList<>(remapRules.length);
        for (final GroupRemapRule rule : remapRules) {
            ret.add(GroupRemapMessage.newBuilder()
            .setTargetGroup(rule.targetGroup)
            .setCondition(marshal(rule.condition))
            .setNegativeGroup(rule.negativeGroup)
            .setPositiveGroup(rule.positiveGroup)
            .build());
        }
        return ret;
    }

    public static TermMessage marshal(final Term term) {
        return TermMessage.newBuilder()
                .setFieldName(term.getFieldName())
                .setIsIntField(term.isIntField())
                .setTermIntVal(term.getTermIntVal())
                .setTermStringVal(term.getTermStringVal())
                .build();
    }

    public static QueryMessage marshal(final Query query) {
        switch (query.getQueryType()) {
            case TERM:
                return QueryMessage.newBuilder().setMinTerm(marshal(query.getStartTerm())).build();
            case BOOLEAN:
                final QueryMessage.Builder builder = QueryMessage.newBuilder()
                        .setOperator(operatorMap.get(query.getOperator()));
                for (final Query operand : query.getOperands()) {
                    builder.addOperand(marshal(operand));
                }
                return builder.build();
            case RANGE:
                return QueryMessage.newBuilder()
                        .setMinTerm(marshal(query.getStartTerm()))
                        .setMaxTerm(marshal(query.getEndTerm()))
                        .setIsMaxInclusive(query.isMaxInclusive())
                        .build();
            default:
                throw new IllegalArgumentException("unrecognized query type: " + query.getQueryType());
        }
    }

    public static QueryRemapMessage marshal(final QueryRemapRule remapRule) {
        return QueryRemapMessage.newBuilder()
                .setTargetGroup(remapRule.getTargetGroup())
                .setQuery(marshal(remapRule.getQuery()))
                .setNegativeGroup(remapRule.getNegativeGroup())
                .setPositiveGroup(remapRule.getPositiveGroup())
                .build();
    }

    public static GroupMultiRemapMessage marshal(final GroupMultiRemapRule rule) {
        final GroupMultiRemapMessage.Builder builder = GroupMultiRemapMessage.newBuilder();
        builder.setNegativeGroup(rule.negativeGroup).setTargetGroup(rule.targetGroup);
        final int numConditions = rule.conditions.length;
        for (int conditionIx = 0; conditionIx < numConditions; conditionIx++) {
            builder.addCondition(marshal(rule.conditions[conditionIx]));
            builder.addPositiveGroup(rule.positiveGroups[conditionIx]);
        }
        return builder.build();
    }

    public static List<GroupMultiRemapMessage> marshal(final GroupMultiRemapRule[] rawRules) {
        final List<GroupMultiRemapMessage> result = Lists.newArrayListWithCapacity(rawRules.length);
        for (final GroupMultiRemapRule rule : rawRules) {
            result.add(marshal(rule));
        }
        return result;
    }

    public static List<TermCount> marshal(final List<TermCountMessage> rawTermCounts) {
        final List<TermCount> ret = Lists.newArrayListWithCapacity(rawTermCounts.size());
        for (final TermCountMessage message : rawTermCounts) {
            ret.add(new TermCount(ImhotepDaemonMarshaller.marshal(message.getTerm()), message.getCount()));
        }
        return ret;
    }

    public static List<RegroupConditionMessage> marshal(final RegroupCondition[] conditions) {
        final List<RegroupConditionMessage> ret = Lists.newArrayList();
        for (final RegroupCondition condition : conditions) {
            ret.add(marshal(condition));
        }
        return ret;
    }

    public static PerformanceStats marshal(final PerformanceStatsMessage stat) {
        final Map<String, Long> customStringStats = new HashMap<>();
        for (final StringLongMessage pairMessage : stat.getCustomStatsList()) {
            customStringStats.put(pairMessage.getKey(), pairMessage.getValue());
        }
        return new PerformanceStats(
                stat.getCpuTime(),
                stat.getMaxMemoryUsed(),
                stat.getFtgsTempFileSize(),
                stat.getFieldFilesReadSize(),
                stat.getCpuSlotsExecTimeMs(),
                stat.getCpuSlotsWaitTimeMs(),
                stat.getIoSlotsExecTimeMs(),
                stat.getIoSlotsWaitTimeMs(),
                ImmutableMap.copyOf(customStringStats));
    }
}
