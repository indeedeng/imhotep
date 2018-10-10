package com.indeed.imhotep.metrics.aggregate;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jwolfe
 */
public class AggregateStatStack {
    private final List<AggregateStat> stack = new ArrayList<>();

    public AggregateStat pop() {
        if (stack.size() == 0) {
            throw new IllegalStateException("no stat to pop");
        }
        return stack.remove(stack.size() - 1);
    }

    public void push(final AggregateStat stat) {
        stack.add(stat);
    }

    public int size() {
        return stack.size();
    }

    public List<AggregateStat> toList() {
        return new ArrayList<>(stack);
    }
}
