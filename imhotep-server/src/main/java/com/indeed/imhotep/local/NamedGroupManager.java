package com.indeed.imhotep.local;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.RegroupParams;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NamedGroupManager implements Closeable {
    // TODO: put SessionID in NDC and remove fields and logging
    private final String sessionId;
    private final MemoryReservationContext memory;
    private final ConcurrentMap<String, GroupLookup> groups;

    public NamedGroupManager(final String sessionId, final MemoryReservationContext memory) {
        this.sessionId = sessionId;
        this.memory = memory;
        this.groups = new ConcurrentHashMap<>();
    }

    public Set<String> groupNames() {
        return Sets.newHashSet(groups.keySet());
    }

    public long getTotalMemoryUsed() {
        long totalMemoryUsed = 0L;
        for (final GroupLookup value : groups.values()) {
            totalMemoryUsed += value.memoryUsed();
        }
        return totalMemoryUsed;
    }

    public void claimMemoryAndPut(final String name, final GroupLookup groupLookup) throws ImhotepOutOfMemoryException {
        if (!memory.claimMemory(groupLookup.memoryUsed())) {
            throw new ImhotepOutOfMemoryException("[" + sessionId + "] Not enough memory to store GroupLookup");
        }
        putAlreadyClaimed(name, groupLookup);
    }

    public void putAlreadyClaimed(final String name, final GroupLookup groupLookup) {
        final GroupLookup oldValue = groups.put(name, groupLookup);
        if (oldValue != null) {
            memory.releaseMemory(oldValue.memoryUsed());
        }
    }

    public void delete(final String name) {
        final GroupLookup oldValue = groups.remove(name);
        if (oldValue != null) {
            memory.releaseMemory(oldValue.memoryUsed());
        }
    }

    public GroupLookup get(final String name) {
        final GroupLookup result = groups.get(name);
        Preconditions.checkArgument(result != null, "The specified groups do not exist: " + name);
        return result;
    }

    /**
     * Creates the given outputGroups to contain all of the same values of inputGroups and be writable at all
     * values from zero to its maxGroup.
     * If inputGroups==outputGroups, then no copying will occur unless resizing is necessary in order to be able to
     * write groups that were not originally settable.
     * If inputGroups!=outputGroups, copying will always occur.
     *
     * @param regroupParams regroup parameters
     * @return the writeable {@link GroupLookup}
     * @throws ImhotepOutOfMemoryException
     */
    public GroupLookup ensureWriteable(final RegroupParams regroupParams) throws ImhotepOutOfMemoryException {
        final GroupLookup inputGroups = get(regroupParams.getInputGroups());
        return ensureWriteable(regroupParams, inputGroups.getNumGroups() - 1);
    }

    /**
     * Creates the given outputGroups to contain all of the same values as inputGroups, and be writable at all
     * values from zero to maxGroup, and returns it.
     * If inputGroups==outputGroups, then no copying will occur unless resizing is necessary in order to be able to
     * write groups that were not originally settable.
     * If inputGroups!=outputGroups, copying will always occur.
     *
     * @param regroupParams regroup parameters
     * @param maxGroup all groups from 0 to this value must be settable in the result
     * @return the writeable {@link GroupLookup}
     * @throws ImhotepOutOfMemoryException
     */
    public GroupLookup ensureWriteable(final RegroupParams regroupParams, final int maxGroup) throws ImhotepOutOfMemoryException {
        if (regroupParams.getInputGroups().equals(regroupParams.getOutputGroups())) {
            resize(regroupParams.getInputGroups(), maxGroup);
            return get(regroupParams.getInputGroups());
        }

        final GroupLookup inputGroups = get(regroupParams.getInputGroups());
        final GroupLookup newGL = GroupLookupFactory.create(maxGroup, inputGroups.size(), memory);
        inputGroups.copyInto(newGL);
        putAlreadyClaimed(regroupParams.getOutputGroups(), newGL);
        return newGL;
    }

    /**
     * Creates the given outputGroups to contain a copy of the inputGroups.
     * If inputGroups==outputGroups, then this operation will just return the original groups.
     */
    public GroupLookup copyInto(final RegroupParams regroupParams) throws ImhotepOutOfMemoryException {
        final GroupLookup inputGroups = get(regroupParams.getInputGroups());
        if (regroupParams.getInputGroups().equals(regroupParams.getOutputGroups())) {
            return inputGroups;
        }

        final GroupLookup outputGroups = inputGroups.makeCopy();
        claimMemoryAndPut(regroupParams.getOutputGroups(), outputGroups);
        return outputGroups;
    }

    /**
     * Resize the named {@link GroupLookup} to ensure that it can support writing all values from
     * 0 to maxGroup, inclusive.
     * @param name group lookup to resize
     * @param maxGroup maximum group that must be settable after this operation
     * @throws ImhotepOutOfMemoryException if allocating exhausts memory
     */
    private void resize(final String name, final int maxGroup) throws ImhotepOutOfMemoryException {
        final GroupLookup lookup = get(name);
        // Deliberately not calling "putAlreadyClaimed" because resize accounted for any freeing necessary.
        groups.put(name, GroupLookupFactory.resize(lookup, maxGroup, memory));
    }

    /**
     * Attempt to shrink the named {@link GroupLookup}.
     * If it is not possible to shrink it, this method has no effect.
     * Counterintuitively, this can throw an ImhotepOutOfMemoryException, because both lookups concurrently exist.
     *
     * @param name the groups to shrink
     * @throws ImhotepOutOfMemoryException if allocating the shrunk {@link GroupLookup} causes ImhotepOutOfMemoryException
     */
    private void tryToShrink(final String name) throws ImhotepOutOfMemoryException {
        final GroupLookup lookup = get(name);
        // Deliberately not calling "putAlreadyClaimed" because resize accounted for any freeing necessary.
        groups.put(name, GroupLookupFactory.resize(lookup, 0, memory, true));
    }

    /**
     * Finalize the regroup by recalculating num groups and returning it.
     * Will also shrink the {@link GroupLookup} if possible, for memory utilization reasons.
     *
     * @param regroupParams Regroup params
     * @return the number of groups after the regroup
     * @throws ImhotepOutOfMemoryException if allocating the shrunk {@link GroupLookup} causes ImhotepOutOfMemoryException
     */
    public int finalizeRegroup(final RegroupParams regroupParams) throws ImhotepOutOfMemoryException {
        final String outputGroups = regroupParams.getOutputGroups();
        final int numGroups;
        {
            final GroupLookup preShrinkGL = get(outputGroups);
            preShrinkGL.recalculateNumGroups();
            numGroups = preShrinkGL.getNumGroups();
        }

        tryToShrink(outputGroups);
        return numGroups;
    }

    /**
     * If all documents are filtered out in the input groups, then create the proper output groups
     * with all documents in group zero and return true.
     * Otherwise, return false;
     * @param regroupParams
     * @return whether or not the regroup should short-circuit
     */
    public boolean handleFiltered(final RegroupParams regroupParams) {
        final GroupLookup inputGroups = get(regroupParams.getInputGroups());
        if (inputGroups.isFilteredOut()) {
            putAlreadyClaimed(regroupParams.getOutputGroups(), new ConstantGroupLookup(0, inputGroups.size()));
            return true;
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        for (final String name : groups.keySet()) {
            delete(name);
        }
    }
}
