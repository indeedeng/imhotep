package com.indeed.imhotep.metrics.aggregate;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.protobuf.AggregateStat;

import java.util.List;
import java.util.stream.Collectors;

import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.ABS;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.AND;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.DIVIDE;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.EQ;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.GT;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.GTE;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.IF_THEN_ELSE;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.LOG;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.LT;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.LTE;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.MAX;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.MIN;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.MINUS;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.MODULUS;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.MULTIPLY;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.NEQ;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.NOT;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.OR;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.PLUS;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.POWER;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.TERM_EQUALS_;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatConstants.TERM_REGEX_;

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
     * A constant (boolean) value
     */
    public static AggregateStatTree constant(boolean value) {
        return of(AggregateStat.newBuilder()
                .setStatType(AggregateStat.StatType.CONSTANT)
                .setValue(value ? 1.0 : 0.0)
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
        return of(operator(TERM_EQUALS_ + "\"" + term + "\""));
    }

    /**
     * 1 if the current term equals the given term, 0 otherwise.
     */
    public static AggregateStatTree termEquals(long term) {
        return of(operator(TERM_EQUALS_ + term));
    }

    /**
     * 1 if the current term equals the given regex, 0 otherwise.
     */
    public static AggregateStatTree termRegex(String regex) {
        return of(operator(TERM_REGEX_ + "\"" + regex + "\""));
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
        return unOp(ABS, this);
    }

    /**
     * Logarithm (base e)
     */
    public AggregateStatTree log() {
        return unOp(LOG, this);
    }

    /**
     * Boolean NOT
     */
    public AggregateStatTree not() {
        return unOp(NOT, this);
    }

    /**
     * Boolean AND
     */
    public AggregateStatTree and(AggregateStatTree other) {
        return binOp(this, AND, other);
    }

    /**
     * Addition
     */
    public AggregateStatTree plus(AggregateStatTree other) {
        return binOp(this, PLUS, other);
    }

    /**
     * Division. this / other.
     */
    public AggregateStatTree divide(AggregateStatTree other) {
        return binOp(this, DIVIDE, other);
    }

    /**
     * Maximum
     */
    public AggregateStatTree max(AggregateStatTree other) {
        return binOp(this, MAX, other);
    }

    /**
     * Minimum
     */
    public AggregateStatTree min(AggregateStatTree other) {
        return binOp(this, MIN, other);
    }

    /**
     * Modulus
     */
    public AggregateStatTree mod(AggregateStatTree other) {
        return binOp(this, MODULUS, other);
    }

    /**
     * Boolean OR
     */
    public AggregateStatTree or(AggregateStatTree other) {
        return binOp(this, OR, other);
    }

    /**
     * Multiplication
     */
    public AggregateStatTree times(AggregateStatTree other) {
        return binOp(this, MULTIPLY, other);
    }

    /**
     * Power. this^other.
     */
    public AggregateStatTree pow(AggregateStatTree other) {
        return binOp(this, POWER, other);
    }

    /**
     * Subtraction. this - other.
     */
    public AggregateStatTree minus(AggregateStatTree other) {
        return binOp(this, MINUS, other);
    }

    /**
     * this &gt; other
     */
    public AggregateStatTree gt(AggregateStatTree other) {
        return binOp(this, GT, other);
    }

    /**
     * this &gt;= other
     */
    public AggregateStatTree gte(AggregateStatTree other) {
        return binOp(this, GTE, other);
    }

    /**
     * this &lt; other
     */
    public AggregateStatTree lt(AggregateStatTree other) {
        return binOp(this, LT, other);
    }

    /**
     * this &lt;= other
     */
    public AggregateStatTree lte(AggregateStatTree other) {
        return binOp(this, LTE, other);
    }

    /**
     * this == other
     */
    public AggregateStatTree eq(AggregateStatTree other) {
        return binOp(this, EQ, other);
    }

    /**
     * this != other
     */
    public AggregateStatTree neq(AggregateStatTree other) {
        return binOp(this, NEQ, other);
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
        builder.add(operator(IF_THEN_ELSE));
        return new AggregateStatTree(builder.build());
    }
}
