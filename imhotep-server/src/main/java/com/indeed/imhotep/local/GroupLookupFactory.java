/*
 * Copyright (C) 2014 Indeed Inc.
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
        GroupLookup createLookup(ImhotepLocalSession session, int size);
        long calcMemUsageForSize(int size);
        int getMaxGroup();
    }

    private static final GroupLookupCreator[] CREATORS = new GroupLookupCreator[] {
        //BitSetGroupLookup
        new GroupLookupCreator() {
            @Override
            public GroupLookup createLookup(final ImhotepLocalSession session, final int size) {
                return new BitSetGroupLookup(session, size);
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
            public GroupLookup createLookup(final ImhotepLocalSession session, final int size) {
                return new ByteGroupLookup(session, size);
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
            public GroupLookup createLookup(final ImhotepLocalSession session, final int size) {
                return new CharGroupLookup(session, size);
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
            public GroupLookup createLookup(final ImhotepLocalSession session, final int size) {
                return new IntGroupLookup(session, size);
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
                                     final ImhotepLocalSession session,
                                     final MemoryReserver memory) throws ImhotepOutOfMemoryException {

        final GroupLookupCreator lookupCreator = findCreator(maxGroup);
        final long memoryUsage = lookupCreator.calcMemUsageForSize(size);
        if (!memory.claimMemory(memoryUsage)) {
            throw new ImhotepOutOfMemoryException();
        }
        return lookupCreator.createLookup(session, size);
    }

    public static GroupLookup resize(final GroupLookup existingGL,
                                     final int maxGroup,
                                     final MemoryReserver memory) throws ImhotepOutOfMemoryException {
        final GroupLookup newGL;

        if (maxGroup > existingGL.maxGroup()) {
            /* need a bigger group */
            newGL = create(maxGroup, existingGL.size(), existingGL.getSession(), memory);
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
            if(!memory.claimMemory(newMemoryUsage)) {
                /* not enough memory to create new lookup. */
                return existingGL;
            }

            newGL = factory.createLookup(existingGL.getSession(), existingGL.size());
        }
        existingGL.copyInto(newGL);
        memory.releaseMemory(existingGL.memoryUsed());
        return newGL;
    }
}
