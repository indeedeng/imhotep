package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author jwolfe
 */
public class ParseAggregate {
    public static final String TERM_EQUALS_ = "term_equals ";
    public static final String TERM_REGEX_ = "term_regex ";

    private static final ImmutableMap<String, Function<AggregateStat, AggregateStat>> UNOPS;
    static {
        final ImmutableMap.Builder<String, Function<AggregateStat, AggregateStat>> unopsBuilder = ImmutableMap.builder();
        unopsBuilder.put("abs", AggregateAbsoluteValue::new);
        unopsBuilder.put("log", AggregateLog::new);
        unopsBuilder.put("not", AggregateNot::new);
        UNOPS = unopsBuilder.build();
    }

    private static final ImmutableMap<String, BiFunction<AggregateStat, AggregateStat, AggregateStat>> BINOPS;

    static {
        final ImmutableMap.Builder<String, BiFunction<AggregateStat, AggregateStat, AggregateStat>> binopsBuilder = ImmutableMap.builder();
        binopsBuilder.put("and", AggregateAnd::new);
        binopsBuilder.put("+", AggregateAddition::new);
        binopsBuilder.put("/", AggregateDivision::new);
        binopsBuilder.put("max", AggregateMax::new);
        binopsBuilder.put("min", AggregateMin::new);
        binopsBuilder.put("%", AggregateModulus::new);
        binopsBuilder.put("or", AggregateOr::new);
        binopsBuilder.put("*", AggregateMultiply::new);
        binopsBuilder.put("^", AggregatePower::new);
        binopsBuilder.put("-", AggregateSubtract::new);
        binopsBuilder.put(">", AggregateGreaterThan::new);
        binopsBuilder.put(">=", AggregateGreaterThanOrEqual::new);
        binopsBuilder.put("<", AggregateLessThan::new);
        binopsBuilder.put("<=", AggregateLessThanOrEqual::new);
        binopsBuilder.put("=", AggregateEqual::new);
        binopsBuilder.put("!=", AggregateNotEqual::new);
        BINOPS = binopsBuilder.build();
    }

    public static class SessionStatsInfo {
        public final int sessionIndex;

        public SessionStatsInfo(int sessionIndex) {
            this.sessionIndex = sessionIndex;
        }
    }

    public static void parse(
            final Map<String, SessionStatsInfo> sessions,
            final AggregateStatStack stack,
            com.indeed.imhotep.protobuf.AggregateStat proto
    ) {
        switch (proto.getStatType()) {
            case OPERATION:
                final String operation = proto.getOperation();
                if (operation.startsWith(TERM_EQUALS_)) {
                    final AggregateStat termEqualsStat;
                    if (operation.charAt(TERM_EQUALS_.length()) == '"') {
                        Preconditions.checkArgument(operation.charAt(operation.length() - 1) == '"');
                        final String termValue = operation.substring(TERM_EQUALS_.length() + 1, operation.length() - 1);
                        termEqualsStat = new AggregateStringTermEquals(termValue);
                    } else {
                        final long termValue = Long.parseLong(operation.substring(TERM_EQUALS_.length()));
                        termEqualsStat = new AggregateIntTermEquals(termValue);
                    }
                    stack.push(termEqualsStat);
                    return;
                }

                if (operation.startsWith(TERM_REGEX_)) {
                    Preconditions.checkArgument(operation.charAt(TERM_REGEX_.length()) == '"');
                    Preconditions.checkArgument(operation.charAt(operation.length() - 1) == '"');
                    final String regex = operation.substring(TERM_REGEX_.length() + 1, operation.length() - 1);
                    stack.push(new AggregateStringTermRegex(regex));
                    return;
                }

                if ("if_then_else".equals(operation)) {
                    final AggregateStat falseCase = stack.pop();
                    final AggregateStat trueCase = stack.pop();
                    final AggregateStat condition = stack.pop();
                    stack.push(new AggregateIfThenElse(condition, trueCase, falseCase));
                    return;
                }

                final BiFunction<AggregateStat, AggregateStat, AggregateStat> binopConstructor = BINOPS.get(operation);
                if (binopConstructor != null) {
                    final AggregateStat rhs = stack.pop();
                    final AggregateStat lhs = stack.pop();
                    stack.push(binopConstructor.apply(lhs, rhs));
                    return;
                }

                final Function<AggregateStat, AggregateStat> unopConstructor = UNOPS.get(operation);
                if (unopConstructor != null) {
                    final AggregateStat wrapped = stack.pop();
                    stack.push(unopConstructor.apply(wrapped));
                    return;
                }


                throw new IllegalArgumentException("Unknown operation type: \"" + operation + "\"");

            case SESSION_STAT:
                final String sessionId = proto.getSessionId();
                final SessionStatsInfo sessionStatsInfo = sessions.get(sessionId);
                final int statIndex = proto.getStatIndex();
                stack.push(new AggregateDocMetric(sessionStatsInfo.sessionIndex, statIndex));
                return;

            case PER_GROUP_VALUE:
                final int numValues = proto.getValuesCount();
                final double[] values = new double[numValues];
                for (int i = 0; i < numValues; i++) {
                    values[i] = proto.getValues(i);
                }
                stack.push(new AggregatePerGroupConstant(values));
                return;

            case CONSTANT:
                stack.push(new AggregateConstant(proto.getValue()));
                return;

            default:
                throw new IllegalArgumentException("Unknown AggregateStat type: " + proto.getStatType());
        }
    }
}
