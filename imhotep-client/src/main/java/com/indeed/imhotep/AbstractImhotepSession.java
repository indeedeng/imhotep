package com.indeed.imhotep;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;

import java.util.Iterator;

/**
 * @author jsadun
 */
public abstract class AbstractImhotepSession implements ImhotepSession {

    public int regroup(final int numRawRules, final Iterator<GroupMultiRemapRule> rawRules) throws ImhotepOutOfMemoryException {
        return regroup(numRawRules, rawRules, false);
    }

    @Override
    public int regroup(int numRules, Iterator<GroupMultiRemapRule> rules, boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        final GroupMultiRemapRule[] rulesArray = new GroupMultiRemapRule[numRules];
        for (int i = 0; i < rulesArray.length; i++) {
            if (!rules.hasNext()) throw new IllegalArgumentException("numRules is "+numRules+" but iterator only had "+i+" rules");
            rulesArray[i] = rules.next();
        }
        return regroup(rulesArray, errorOnCollisions);
    }

    public int regroup2(final int numRules, final Iterator<GroupRemapRule> rules) throws ImhotepOutOfMemoryException {
        final GroupRemapRule[] rulesArray = new GroupRemapRule[numRules];
        for (int i = 0; i < rulesArray.length; i++) {
            if (!rules.hasNext()) throw new IllegalArgumentException("numRules is "+numRules+" but iterator only had "+i+" rules");
            rulesArray[i] = rules.next();
        }
        return regroup(rulesArray);
    }

    public int regroup(final GroupMultiRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        return regroup(rawRules, false);
    }

    @Override
    public int metricRegroup(int stat, long min, long max, long intervalSize) throws ImhotepOutOfMemoryException {
        return metricRegroup(stat, min, max, intervalSize, false);
    }
}
