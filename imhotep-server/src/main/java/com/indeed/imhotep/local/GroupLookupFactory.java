package com.indeed.imhotep.local;

import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;

public class GroupLookupFactory {

    public static GroupLookup create(int maxGroup,
                                     int size,
                                     ImhotepLocalSession session,
                                     MemoryReservationContext memory) throws ImhotepOutOfMemoryException {
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

    public static GroupLookup resize(GroupLookup existingGL,
                                     int maxGroup,
                                     MemoryReservationContext memory) throws ImhotepOutOfMemoryException {
        final GroupLookup newGL;

        if (maxGroup > existingGL.maxGroup()) {
            /* need a bigger group */
            newGL = create(maxGroup, existingGL.size(), existingGL.getSession(), memory);
        } else {
            /* maybe the group lookup can be shrunk */
            int newMaxgroup = Math.max(maxGroup, existingGL.getNumGroups());
            if ((float) newMaxgroup > 0.7f * existingGL.maxGroup()) {
                return existingGL;
            }
    
            /* try to shrink the GroupLookup */
            try {
                newGL = create(newMaxgroup, existingGL.size(), existingGL.getSession(), memory);
            } catch (ImhotepOutOfMemoryException e) {
                return existingGL;
            }
        }
        existingGL.copyInto(newGL);
        memory.releaseMemory(existingGL.memoryUsed());
        return newGL;
    }
}
