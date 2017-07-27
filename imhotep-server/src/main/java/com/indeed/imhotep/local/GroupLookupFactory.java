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

import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;

public class GroupLookupFactory {
    private GroupLookupFactory() {
    }

    public static GroupLookup create(final int maxGroup,
                                     final int size,
                                     final ImhotepLocalSession session,
                                     final MemoryReservationContext memory) throws ImhotepOutOfMemoryException {
        final GroupLookup newLookup;
        if (maxGroup < 2) { // 8L * ((size + 64) >> 6)
            if (!memory.claimMemory(BitSetGroupLookup.calcMemUsageForSize(size))) {
                throw new ImhotepOutOfMemoryException();
            }
            newLookup = new BitSetGroupLookup(session, size);
        } else if (maxGroup < 256) {
            if (!memory.claimMemory(ByteGroupLookup.calcMemUsageForSize(size))) {
                throw new ImhotepOutOfMemoryException();
            }
            newLookup = new ByteGroupLookup(session, size);
        } else if (maxGroup < 65536) {
            if (!memory.claimMemory(CharGroupLookup.calcMemUsageForSize(size))) {
                throw new ImhotepOutOfMemoryException();
            }
            newLookup = new CharGroupLookup(session, size);
        } else {
            if (!memory.claimMemory(IntGroupLookup.calcMemUsageForSize(size))) {
                throw new ImhotepOutOfMemoryException();
            }
            newLookup = new IntGroupLookup(session, size);
        }

        return newLookup;
    }

    public static GroupLookup resize(final GroupLookup existingGL,
                                     final int maxGroup,
                                     final MemoryReservationContext memory) throws ImhotepOutOfMemoryException {
        final GroupLookup newGL;

        if (maxGroup > existingGL.maxGroup()) {
            /* need a bigger group */
            newGL = create(maxGroup, existingGL.size(), existingGL.getSession(), memory);
        } else {
            /* maybe the group lookup can be shrunk */
            final int newMaxgroup = Math.max(maxGroup, existingGL.getNumGroups());
            if ((float) newMaxgroup > 0.7f * existingGL.maxGroup()) {
                return existingGL;
            }
    
            /* try to shrink the GroupLookup */
            try {
                newGL = create(newMaxgroup, existingGL.size(), existingGL.getSession(), memory);
            } catch (final ImhotepOutOfMemoryException e) {
                return existingGL;
            }
        }
        existingGL.copyInto(newGL);
        memory.releaseMemory(existingGL.memoryUsed());
        return newGL;
    }
}
