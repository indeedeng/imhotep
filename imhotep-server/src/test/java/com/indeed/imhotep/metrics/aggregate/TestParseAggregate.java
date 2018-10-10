package com.indeed.imhotep.metrics.aggregate;

import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author jwolfe
 */
public class TestParseAggregate {
    private static class Opaque implements AggregateStat {
        @Override
        public double apply(MultiFTGSIterator multiFTGSIterator) {
            return 0;
        }
    }

    private void testUnop(
            final Function<AggregateStat, AggregateStat> constructor,
            final String operation
    ) {
        final Opaque wrapped = new Opaque();
        final AggregateStatStack stack = new AggregateStatStack();
        stack.push(wrapped);
        final com.indeed.imhotep.protobuf.AggregateStat.Builder proto = com.indeed.imhotep.protobuf.AggregateStat.newBuilder();
        proto.setStatType(com.indeed.imhotep.protobuf.AggregateStat.StatType.OPERATION);
        proto.setOperation(operation);
        ParseAggregate.parse(null, stack, proto.build());
        Assert.assertEquals(1, stack.size());
        Assert.assertEquals(constructor.apply(wrapped), stack.pop());
    }

    private void testBinop(
            final BiFunction<AggregateStat, AggregateStat, AggregateStat> constructor,
            final String operation
    ) {
        final Opaque lhs = new Opaque();
        final Opaque rhs = new Opaque();
        final AggregateStatStack stack = new AggregateStatStack();
        stack.push(lhs);
        stack.push(rhs);
        final com.indeed.imhotep.protobuf.AggregateStat.Builder proto = com.indeed.imhotep.protobuf.AggregateStat.newBuilder();
        proto.setStatType(com.indeed.imhotep.protobuf.AggregateStat.StatType.OPERATION);
        proto.setOperation(operation);
        ParseAggregate.parse(null, stack, proto.build());
        Assert.assertEquals(1, stack.size());
        final AggregateStat result = stack.pop();
        Assert.assertEquals(constructor.apply(lhs, rhs), result);
        // cheap check that we're not false positive-ing all over the place
        Assert.assertFalse(constructor.apply(rhs, lhs).equals(result));
    }

    @Test
    public void testParseOperations() {
        // TODO: term_equals
        // TODO: term_regex
        // TODO: if_then_else

        testBinop(AggregateAnd::new, "and");
        testBinop(AggregateAddition::new, "+");
        testBinop(AggregateDivision::new, "/");
        testBinop(AggregateMax::new, "max");
        testBinop(AggregateMin::new, "min");
        testBinop(AggregateModulus::new, "%");
        testBinop(AggregateOr::new, "or");
        testBinop(AggregateMultiply::new, "*");
        testBinop(AggregatePower::new, "^");
        testBinop(AggregateSubtract::new, "-");
        testBinop(AggregateGreaterThan::new, ">");
        testBinop(AggregateGreaterThanOrEqual::new, ">=");
        testBinop(AggregateLessThan::new, "<");
        testBinop(AggregateLessThanOrEqual::new, "<=");
        testBinop(AggregateEqual::new, "=");
        testBinop(AggregateNotEqual::new, "!=");

        testUnop(AggregateAbsoluteValue::new, "abs");
        testUnop(AggregateLog::new, "log");
        testUnop(AggregateNot::new, "not");

    }
}