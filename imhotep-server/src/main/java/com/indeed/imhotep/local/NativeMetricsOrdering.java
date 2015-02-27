package com.indeed.imhotep.local;

import com.indeed.flamdex.api.IntValueLookup;

import java.util.List;

/**
 * Created by darren on 2/27/15.
 */
public class NativeMetricsOrdering {


    public static class StatsOrderingInfo {
        List<IntValueLookup> reorderedMetrics;
        int[] mins;
        int[] maxes;
    }


    public StatsOrderingInfo getOrder(List<IntValueLookup> metrics) {
        return null;
    }

}
