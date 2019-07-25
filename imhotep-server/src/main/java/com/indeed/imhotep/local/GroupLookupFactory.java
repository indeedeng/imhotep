/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.imhotep.local;

import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;

public class GroupLookupFactory {
    private GroupLookupFactory() {
    }

    private interface GroupLookupCreator {
        GroupLookup createLookup(int size);
        long calcMemUsageForSize(int size);
        int getMaxGroup();
    }

    private static final GroupLookupCreator[] CREATORS = new GroupLookupCreator[] {
        //BitSetGroupLookup
        new GroupLookupCreator() {
            @Override
            public GroupLookup createLookup(final int size) {
                return new BitSetGroupLookup(size);
            }

            @Override
            public long calcMemUsageForSize(final int size) {
                return BitSetGroupLookup.calcMemUsageForSize(size);
            }

            @Override
            public int getMaxGroup() {
                return 1;
            }
        },

        //ByteGroupLookup
        new GroupLookupCreator() {
            @Override
            public GroupLookup createLookup(final int size) {
                return new ByteGroupLookup(size);
            }

            @Override
            public long calcMemUsageForSize(final int size) {
                return ByteGroupLookup.calcMemUsageForSize(size);
            }

            @Override
            public int getMaxGroup() {
                return 255;
            }
        },

        //CharGroupLookup
        new GroupLookupCreator() {
            @Override
            public GroupLookup createLookup(final int size) {
                return new CharGroupLookup(size);
            }

            @Override
            public long calcMemUsageForSize(final int size) {
                return CharGroupLookup.calcMemUsageForSize(size);
            }

            @Override
            public int getMaxGroup() {
                return 65535;
            }
        },

        //IntGroupLookup
        new GroupLookupCreator() {
            @Override
            public GroupLookup createLookup(final int size) {
                return new IntGroupLookup(size);
            }

            @Override
            public long calcMemUsageForSize(final int size) {
                return IntGroupLookup.calcMemUsageForSize(size);
            }

            @Override
            public int getMaxGroup() {
                return Integer.MAX_VALUE;
            }
        }
    };

    private static GroupLookupCreator findCreator(final int groupCount) {
        for(final GroupLookupCreator creator : CREATORS) {
            if( groupCount <= creator.getMaxGroup()) {
                return creator;
            }
        }

        throw new RuntimeException("Unreachable code in GroupLookupCreator.findCreator");
    }

    public static GroupLookup create(final int maxGroup,
                                     final int size,
                                     final MemoryReserver memory) throws ImhotepOutOfMemoryException {

        final GroupLookupCreator lookupCreator = findCreator(maxGroup);
        final long memoryUsage = lookupCreator.calcMemUsageForSize(size);
        memory.claimMemoryOrThrowIOOME(memoryUsage);
        return lookupCreator.createLookup(size);
    }

    public static GroupLookup resize(final GroupLookup existingGL,
                                     final int maxGroup,
                                     final MemoryReserver memory) throws ImhotepOutOfMemoryException {
        return resize(existingGL, maxGroup, memory, false);
    }

    // This method is used in two different fashions.
    // The first is to ensure that the group lookup is capable of setting values in anticipation of performing a regroup
    // with a series of set() calls.
    // The second is to attempt to free up some memory *after* performing a regroup when we notice dynamically that
    // we've ended up creating a group lookup that supports more than we need.
    // In order to disambiguate the two cases, we add the shrinkOnly parameter to use when we're attempting to shrink
    // but do not care whether it's valid to set values up to the given maxGroup.
    public static GroupLookup resize(final GroupLookup existingGL,
                                     final int maxGroup,
                                     final MemoryReserver memory,
                                     final boolean shrinkOnly) throws ImhotepOutOfMemoryException {
        final GroupLookup newGL;

        if (!shrinkOnly && ((maxGroup > existingGL.maxGroup()) || !existingGL.canRepresentAllValuesUpToMaxGroup())) {
            /* need a bigger group or the ability to represent all values */
            newGL = create(Math.max(maxGroup, existingGL.maxGroup()), existingGL.size(), memory);
        } else {
            /* check if the group lookup can be shrunk */
            final int newMaxgroup = Math.max(maxGroup, existingGL.getNumGroups());
            final GroupLookupCreator factory = findCreator(newMaxgroup);
            final long newMemoryUsage = factory.calcMemUsageForSize(existingGL.size());
            if(newMemoryUsage >= existingGL.memoryUsed()) {
                /* can't decrease memory by using another lookup */
                return existingGL;
            }

            /* try to shrink the GroupLookup */
            if(memory.claimMemory(newMemoryUsage) != MemoryReserver.AllocationResult.ALLOCATED) {
                /* not enough memory to create new lookup. */
                return existingGL;
            }

            newGL = factory.createLookup(existingGL.size());
        }
        existingGL.copyInto(newGL);
        memory.releaseMemory(existingGL.memoryUsed());
        return newGL;
    }
}
