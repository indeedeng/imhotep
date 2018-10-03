package com.indeed.imhotep.metrics.aggregate;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.protobuf.AggregateStat;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author jwolfe
 */
public class AggregateStatTree {
    private final ImmutableList<AggregateStat> pushes;

    private AggregateStatTree(ImmutableList<AggregateStat> pushes) {
        this.pushes = pushes;
    }

    /**
     * Reference to the sum of a document level stat from a particular session.
     *
     * Roughly corresponds to square brackets in IQL2.
     *
     * The corresponding stat must still occupy the same position on the stack when
     * any trees containing this AggregateStatTree are used in an operation.
     *
     * Nothing prevents you from shooting yourself in the foot with
     * session.pushStat(...)
     * AggregateStatTree stat = AggregateStatTree.stat(session, 0);
     * session.popStat();
     * [...]
     * session.something(stat);
     */
    public static AggregateStatTree stat(ImhotepSession session, int statIndex) {
        return of(AggregateStat.newBuilder()
                .setStatType(AggregateStat.StatType.SESSION_STAT)
                .setSessionId(session.getSessionId())
                .setStatIndex(statIndex)
                .build());
    }

    /**
     * A constant value
     */
    public static AggregateStatTree constant(double value) {
        return of(AggregateStat.newBuilder()
                .setStatType(AggregateStat.StatType.CONSTANT)
                .setValue(value)
                .build());
    }

    /**
     * A per-group constant value
     */
    public static AggregateStatTree perGroupConstant(double[] values) {
        return of(AggregateStat.newBuilder()
                .setStatType(AggregateStat.StatType.PER_GROUP_VALUE)
                .addAllValues(Doubles.asList(values))
                .build());
    }

    /**
     * 1 if the current term equals the given term, 0 otherwise.
     */
    public static AggregateStatTree termEquals(String term) {
        return of(operator("term_equals \"" + term + "\""));
    }

    /**
     * 1 if the current term equals the given term, 0 otherwise.
     */
    public static AggregateStatTree termEquals(long term) {
        return of(operator("term_equals " + term));
    }

    /**
     * 1 if the current term equals the given regex, 0 otherwise.
     */
    public static AggregateStatTree termRegex(String regex) {
        return of(operator("term_regex \"" + regex + "\""));
    }

    public List<AggregateStat> asList() {
        return pushes;
    }

    public static List<AggregateStat> allAsList(List<AggregateStatTree> trees) {
        return trees.stream().flatMap(x -> x.asList().stream()).collect(Collectors.toList());
    }

    /**
     * Absolute value
     */
    public AggregateStatTree abs() {
        return unOp("abs", this);
    }

    /**
     * Logarithm (base e)
     */
    public AggregateStatTree log() {
        return unOp("log", this);
    }

    /**
     * Boolean NOT
     */
    public AggregateStatTree not() {
        return unOp("not", this);
    }

    /**
     * Boolean AND
     */
    public AggregateStatTree and(AggregateStatTree other) {
        return binOp(this, "and", other);
    }

    /**
     * Addition
     */
    public AggregateStatTree plus(AggregateStatTree other) {
        return binOp(this, "+", other);
    }

    /**
     * Division. this / other.
     */
    public AggregateStatTree divide(AggregateStatTree other) {
        return binOp(this, "/", other);
    }

    /**
     * Maximum
     */
    public AggregateStatTree max(AggregateStatTree other) {
        return binOp(this, "max", other);
    }

    /**
     * Minimum
     */
    public AggregateStatTree min(AggregateStatTree other) {
        return binOp(this, "min", other);
    }

    /**
     * Modulus
     */
    public AggregateStatTree mod(AggregateStatTree other) {
        return binOp(this, "%", other);
    }

    /**
     * Boolean OR
     */
    public AggregateStatTree or(AggregateStatTree other) {
        return binOp(this, "or", other);
    }

    /**
     * Multiplication
     */
    public AggregateStatTree times(AggregateStatTree other) {
        return binOp(this, "*", other);
    }

    /**
     * Power. this^other.
     */
    public AggregateStatTree pow(AggregateStatTree other) {
        return binOp(this, "^", other);
    }

    /**
     * Subtraction. this - other.
     */
    public AggregateStatTree minus(AggregateStatTree other) {
        return binOp(this, "-", other);
    }

    /**
     * this > other
     */
    public AggregateStatTree gt(AggregateStatTree other) {
        return binOp(this, ">", other);
    }

    /**
     * this >= other
     */
    public AggregateStatTree gte(AggregateStatTree other) {
        return binOp(this, ">=", other);
    }

    /**
     * this < other
     */
    public AggregateStatTree lt(AggregateStatTree other) {
        return binOp(this, "<", other);
    }

    /**
     * this <= other
     */
    public AggregateStatTree lte(AggregateStatTree other) {
        return binOp(this, "<=", other);
    }

    /**
     * this == other
     */
    public AggregateStatTree eq(AggregateStatTree other) {
        return binOp(this, "=", other);
    }

    /**
     * this != other
     */
    public AggregateStatTree neq(AggregateStatTree other) {
        return binOp(this, "!=", other);
    }

    /**
     * Conditionally choose one value or the other.
     * this ? ifTrue : ifFalse
     */
    public AggregateStatTree conditional(AggregateStatTree ifTrue, AggregateStatTree ifFalse) {
        return ifThenElse(this, ifTrue, ifFalse);
    }

    // Things that deal with protobufs

    private static AggregateStatTree of(AggregateStat.Builder builder) {
        return AggregateStatTree.of(builder.build());
    }

    private static AggregateStatTree of(AggregateStat proto) {
        return new AggregateStatTree(ImmutableList.of(proto));
    }

    private static AggregateStat operator(String operator) {
        return AggregateStat.newBuilder().setStatType(AggregateStat.StatType.OPERATION).setOperation(operator).build();
    }

    private static AggregateStatTree unOp(String operator, AggregateStatTree arg) {
        final ImmutableList.Builder<AggregateStat> builder = new ImmutableList.Builder<>();
        builder.addAll(arg.pushes);
        builder.add(operator(operator));
        return new AggregateStatTree(builder.build());
    }

    private static AggregateStatTree binOp(AggregateStatTree lhs, String operator, AggregateStatTree rhs) {
        final ImmutableList.Builder<AggregateStat> builder = new ImmutableList.Builder<>();
        builder.addAll(lhs.pushes);
        builder.addAll(rhs.pushes);
        builder.add(operator(operator));
        return new AggregateStatTree(builder.build());
    }

    private static AggregateStatTree ifThenElse(AggregateStatTree cond, AggregateStatTree trueCase, AggregateStatTree falseCase) {
        final ImmutableList.Builder<AggregateStat> builder = new ImmutableList.Builder<>();
        builder.addAll(cond.pushes);
        builder.addAll(trueCase.pushes);
        builder.addAll(falseCase.pushes);
        builder.add(operator("if_then_else"));
        return new AggregateStatTree(builder.build());
    }
}
