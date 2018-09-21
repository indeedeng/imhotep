package com.indeed.imhotep.multisession;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.indeed.imhotep.api.FTGAIterator;
import com.indeed.imhotep.metrics.aggregate.AggregateConstant;
import com.indeed.imhotep.metrics.aggregate.AggregateDocMetric;
import com.indeed.imhotep.metrics.aggregate.AggregateEqual;
import com.indeed.imhotep.metrics.aggregate.AggregateModulus;
import com.indeed.imhotep.metrics.aggregate.AggregateMultiply;
import com.indeed.imhotep.metrics.aggregate.AggregateStat;
import com.indeed.imhotep.metrics.aggregate.MultiFTGSIterator;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static org.easymock.EasyMock.checkOrder;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

/**
 * @author jwolfe
 */
public class MultiSessionWrapperTest {

    private static class TermData {
        private final long term;
        private final List<GroupData> groupData;

        private TermData(long term, List<GroupData> groupData) {
            this.term = term;
            this.groupData = groupData;
        }
    }

    private static class GroupData {
        private final int group;
        private final List<Long> stats;

        private GroupData(int group, List<Long> stats) {
            this.group = group;
            this.stats = stats;
        }
    }


    private static MultiFTGSIterator makeMock(List<TermData> tgses) {
        final MultiFTGSIterator input = createMock(MultiFTGSIterator.class);

        expect(input.nextField()).andReturn(true);

        for (final TermData tgs : tgses) {
            checkOrder(input, true);
            expect(input.nextTerm()).andReturn(true);

            for (final GroupData groupData : tgs.groupData) {
                checkOrder(input, true);
                expect(input.nextGroup()).andReturn(true);

                checkOrder(input, false);
                expect(input.termIntVal()).andReturn(tgs.term).anyTimes();
                expect(input.group()).andReturn(groupData.group).anyTimes();
                expect(input.groupStat(EasyMock.eq(0), EasyMock.anyInt()))
                        .andAnswer(() -> {
                            final Integer index = (Integer) EasyMock.getCurrentArguments()[1];
                            return groupData.stats.get(index);
                        }).anyTimes();
            }

            checkOrder(input, true);
            expect(input.nextGroup()).andReturn(false);
        }

        checkOrder(input, true);
        expect(input.nextTerm()).andReturn(false);
        expect(input.nextField()).andReturn(false);

        replay(input);
        return input;
    }

    private static List<TGS> simulate(
            final List<TermData> terms,
            final BiFunction<Long, GroupData, Optional<List<Double>>> expectedBehavior
    ) {
        final List<TGS> result = new ArrayList<>();
        for (final TermData term : terms) {
            for (final GroupData groupData : term.groupData) {
                final Optional<List<Double>> entry = expectedBehavior.apply(term.term, groupData);
                entry.ifPresent(stats -> result.add(new TGS(term.term, groupData.group, stats)));
            }
        }
        return result;
    }

    @Test
    public void test() {
        verifySimulation(Collections.emptyList(), (x, y) -> Optional.empty(), Collections.emptyList(), Collections.emptyList());

        final ArrayList<TermData> data = Lists.newArrayList(
                new TermData(1, Lists.newArrayList(
                        new GroupData(1, Lists.newArrayList(11L)),
                        new GroupData(2, Lists.newArrayList(12L)))
                ),
                new TermData(2, Lists.newArrayList(
                        new GroupData(1, Lists.newArrayList(21L)),
                        new GroupData(3, Lists.newArrayList(23L))
                ))
        );

        verifySimulation(
                data,
                (term, groupData) -> groupData.stats.get(0) % 2 == 1 ? Optional.of(Lists.newArrayList(2.0 * groupData.stats.get(0))) : Optional.empty(),
                Lists.newArrayList(
                        new AggregateEqual(
                                new AggregateConstant(1),
                                new AggregateModulus(
                                        new AggregateDocMetric(0, 0),
                                        new AggregateConstant(2)))),
                Lists.newArrayList(
                        new AggregateMultiply(
                                new AggregateConstant(2),
                                new AggregateDocMetric(0, 0)))
        );
    }

    private void verifySimulation(List<TermData> data, BiFunction<Long, GroupData, Optional<List<Double>>> expectedBehavior, List<AggregateStat> filters, List<AggregateStat> selects) {
        final List<TGS> expected = simulate(data, expectedBehavior);
        final MultiFTGSIterator mock = makeMock(data);
        final MultiSessionWrapper iterator = new MultiSessionWrapper(mock, filters, selects);
        final List<TGS> actual = traverse(iterator);
        Assert.assertEquals(
                expected,
                actual
        );
        EasyMock.verify(mock);
    }

    private static class TGS {
        private final long term;
        private final int group;
        private final List<Double> stats;

        private TGS(long term, int group, List<Double> stats) {
            this.term = term;
            this.group = group;
            this.stats = stats;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TGS tgs = (TGS) o;
            return term == tgs.term &&
                    group == tgs.group &&
                    Objects.equal(stats, tgs.stats);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(term, group, stats);
        }

        @Override
        public String toString() {
            return "TGS{" +
                    "term=" + term +
                    ", group=" + group +
                    ", stats=" + stats +
                    '}';
        }
    }

    private static List<TGS> traverse(FTGAIterator iterator) {
        final List<TGS> result = new ArrayList<>();
        final double[] statsBuf = new double[iterator.getNumStats()];
        while (iterator.nextField()) {
            while (iterator.nextTerm()) {
                while (iterator.nextGroup()) {
                    iterator.groupStats(statsBuf);
                    result.add(new TGS(iterator.termIntVal(), iterator.group(), Lists.newArrayList(Doubles.asList(statsBuf))));
                }
            }
        }
        return result;
    }
}