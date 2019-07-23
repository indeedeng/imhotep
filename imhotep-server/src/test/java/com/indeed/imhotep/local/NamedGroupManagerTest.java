package com.indeed.imhotep.local;

import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.RegroupParams;
import org.junit.Assert;
import org.junit.Test;

public class NamedGroupManagerTest {

    public static final MemoryReserver NO_OP_MEMORY_RESERVER = new MemoryReserver() {
        @Override
        public long usedMemory() {
            return 0;
        }

        @Override
        public long totalMemory() {
            return 0;
        }

        @Override
        public boolean claimMemory(final long numBytes) {
            return true;
        }

        @Override
        public void releaseMemory(final long numBytes) {
        }

        @Override
        public void close() {
        }
    };

    @Test
    public void testEnsureWriteable() throws ImhotepOutOfMemoryException {
        final NamedGroupManager namedGroupManager = new NamedGroupManager(new MemoryReservationContext(NO_OP_MEMORY_RESERVER));

        final ConstantGroupLookup g1 = new ConstantGroupLookup(1, 100);
        namedGroupManager.put("g1", g1);

        final GroupLookup g2 = namedGroupManager.ensureWriteable(new RegroupParams("g1", "g1"));
        Assert.assertNotSame("ConstantGroupLookup is not writeable", g1, g2);
        Assert.assertTrue(g2 instanceof BitSetGroupLookup);

        final GroupLookup g3 = namedGroupManager.ensureWriteable(new RegroupParams("g1", "g1"));
        Assert.assertSame("expect ensureWriteable to return same object for inputGroups==outputGroups", g2, g3);
        Assert.assertTrue(g3 instanceof BitSetGroupLookup);

        final GroupLookup g4 = namedGroupManager.ensureWriteable(new RegroupParams("g1", "g1"), 150);
        Assert.assertSame("expect ensureWriteable to overwrite and resize", namedGroupManager.get("g1"), g4);
        Assert.assertTrue(g4 instanceof ByteGroupLookup);

        g4.set(5, 150);

        final GroupLookup g5 = namedGroupManager.ensureWriteable(new RegroupParams("g1", "g1"), 1);
        Assert.assertSame(g4, g5);

        final GroupLookup g6 = namedGroupManager.ensureWriteable(new RegroupParams("g1", "g2"), 1);
        Assert.assertNotSame(g5, g6);
        Assert.assertSame(g6, namedGroupManager.get("g2"));
        Assert.assertTrue(g6 instanceof ByteGroupLookup);
    }

    @Test
    public void testGetPutDelete() throws ImhotepOutOfMemoryException {
        final MemoryReservationContext memory = new MemoryReservationContext(new ImhotepMemoryPool(10_000));
        final NamedGroupManager namedGroupManager = new NamedGroupManager(memory);

        try {
            namedGroupManager.get("unknown_groups");
            Assert.fail("Expected exception");
        } catch (final IllegalArgumentException e) {
            Assert.assertEquals("The specified groups do not exist: unknown_groups", e.getMessage());
        }

        final ConstantGroupLookup a = new ConstantGroupLookup(1, 500);
        namedGroupManager.put("a", a);
        Assert.assertEquals(0L, memory.usedMemory());
        Assert.assertSame(a, namedGroupManager.get("a"));

        final ByteGroupLookup b = new ByteGroupLookup(500);

        memory.claimMemoryOrThrowIOOME(b.memoryUsed());
        namedGroupManager.put("b", b);
        Assert.assertEquals(b.memoryUsed(), memory.usedMemory());
        Assert.assertSame(b, namedGroupManager.get("b"));

        namedGroupManager.put("b", new ConstantGroupLookup(1, 500));
        Assert.assertEquals(0, memory.usedMemory());

        memory.claimMemoryOrThrowIOOME(b.memoryUsed());
        namedGroupManager.put("b", b);
        Assert.assertEquals(b.memoryUsed(), memory.usedMemory());

        namedGroupManager.delete("b");
        Assert.assertEquals(0, memory.usedMemory());
    }

    @Test
    public void testCopyInto() throws ImhotepOutOfMemoryException {
        final MemoryReservationContext memory = new MemoryReservationContext(new ImhotepMemoryPool(10_000));
        final NamedGroupManager namedGroupManager = new NamedGroupManager(memory);

        namedGroupManager.put("a", new ConstantGroupLookup(1, 500));
        Assert.assertEquals(0L, memory.usedMemory());

        final ByteGroupLookup b = new ByteGroupLookup(500);

        memory.claimMemoryOrThrowIOOME(b.memoryUsed());
        namedGroupManager.put("b", b);

        final GroupLookup b2 = namedGroupManager.copyInto(new RegroupParams("b", "b2"));
        Assert.assertSame(b2, namedGroupManager.get("b2"));
        Assert.assertNotSame(b, b2);

        Assert.assertEquals(2 * b.memoryUsed(), memory.usedMemory());
    }
}