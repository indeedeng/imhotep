package com.indeed.imhotep.multisession;

import com.indeed.imhotep.metrics.aggregate.AggregateStat;
import com.indeed.imhotep.metrics.aggregate.MultiFTGSIterator;
import com.indeed.imhotep.service.FTGSOutputStreamWriter;

import java.io.IOException;
import java.util.List;

/**
 * @author jwolfe
 */
public class MultiSessionProcessor {
    public static void process(
            final MultiFTGSIterator multiIterator,
            final List<AggregateStat> filters,
            final List<AggregateStat> selects,
            final FTGSOutputStreamWriter writer
    ) throws IOException {
        while (multiIterator.nextField()) {
            writer.switchField(multiIterator.fieldName(), multiIterator.fieldIsIntType());
            while (multiIterator.nextTerm()) {
                while (multiIterator.nextGroup()) {
                    processGroups(
                            multiIterator,
                            filters,
                            selects,
                            writer
                    );
                }
            }
        }
    }

    private static boolean allFiltersPass(
            final MultiFTGSIterator multiIterator,
            final List<AggregateStat> filters
    ) {
        for (final AggregateStat filter : filters) {
            if (!AggregateStat.truthy(filter.apply(multiIterator))) {
                return false;
            }
        }
        return true;
    }

    private static void processGroups(
            final MultiFTGSIterator multiIterator,
            final List<AggregateStat> filters,
            final List<AggregateStat> selects,
            final FTGSOutputStreamWriter writer
    ) throws IOException {
        boolean termWritten = false;
        while (multiIterator.nextGroup()) {
            if (!allFiltersPass(multiIterator, filters)) {
                continue;
            }

            if (!termWritten) {
                final long docFreq = multiIterator.termDocFreq();
                if (multiIterator.fieldIsIntType()) {
                    writer.switchIntTerm(multiIterator.termIntVal(), docFreq);
                } else {
                    writer.switchBytesTerm(
                            multiIterator.termStringBytes(),
                            multiIterator.termStringLength(),
                            docFreq
                    );
                }
                termWritten = true;
            }

            writer.switchGroup(multiIterator.group());
            for (final AggregateStat select : selects) {
                writer.addStat(select.apply(multiIterator));
            }
        }
    }
}
