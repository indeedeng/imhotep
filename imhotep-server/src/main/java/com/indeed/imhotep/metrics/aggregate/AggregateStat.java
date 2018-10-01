package com.indeed.imhotep.metrics.aggregate;

/**
 * @author jwolfe
 */
public interface AggregateStat {
    double apply(MultiFTGSIterator multiFTGSIterator);

    static boolean truthy(double value) {
        return value != 0.0;
    }

    static double floaty(boolean value) {
        return value ? 1.0 : 0.0;
    }
}
