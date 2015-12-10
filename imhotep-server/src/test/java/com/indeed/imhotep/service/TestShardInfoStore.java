package com.indeed.imhotep.service;

import com.indeed.imhotep.ShardInfo;
import com.indeed.util.io.Files;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

public class TestShardInfoStore {

    private static final Random rng = new Random(0xdeadbeef);

    @Test public void testRoundTrip() {

        String tmp = null;
        try {
            tmp = Files.getTempDirectory(TestShardInfoStore.class.getSimpleName(),
                                         "delete.me");

            HashSet<ShardInfo> before = new HashSet<ShardInfo>();
            for (int count = 0; count < 4096; ++count) {
                before.add(new ShardInfo(RandomStringUtils.randomAlphanumeric(32),
                                         RandomStringUtils.randomAlphanumeric(rng.nextInt(256)),
                                         null, rng.nextInt(), rng.nextLong()));
            }

            /* Create the store and verify that it has everything in it. */
            try (ShardInfoStore store = new ShardInfoStore(new File(tmp))) {
                    for (ShardInfo shardInfo: before) {
                        store.put(shardInfo);
                    }

                    HashSet<ShardInfo> after = new HashSet<ShardInfo>();
                    Iterator<ShardInfo> it = store.iterator();
                    while (it.hasNext()) {
                        after.add(it.next());
                    }

                    assertEquals(before, after);
                }

            /* Reopen the store and verify that it has everything in it. */
            try (ShardInfoStore store = new ShardInfoStore(new File(tmp))) {
                    HashSet<ShardInfo> after = new HashSet<ShardInfo>();
                    Iterator<ShardInfo> it = store.iterator();
                    while (it.hasNext()) {
                        after.add(it.next());
                    }

                    assertEquals(before, after);
                }
        }
        catch (IOException ex) {
            fail(ex.toString());
        }
        finally {
            if (tmp != null) Files.delete(tmp);
        }
    }
}
