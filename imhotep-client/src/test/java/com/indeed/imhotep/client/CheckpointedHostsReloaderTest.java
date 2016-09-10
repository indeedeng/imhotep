package com.indeed.imhotep.client;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * @author kenh
 */

public class CheckpointedHostsReloaderTest {
    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testLoad() throws IOException {
        final File hostsFile = new File(tempDir.getRoot(), "hosts.dat");
        // loading
        {
            final CheckpointedHostsReloader reloader = new CheckpointedHostsReloader(hostsFile, new DummyHostsReloader(
                    Collections.<Host>emptyList()), 0.5);
            reloader.run();
            Assert.assertFalse(reloader.isLoadedDataSuccessfullyRecently());
        }

        {
            final CheckpointedHostsReloader reloader = new CheckpointedHostsReloader(hostsFile, new DummyHostsReloader(
                    Arrays.asList(
                            new Host("H1", 8080),
                            new Host("H2", 8080),
                            new Host("H3", 8080),
                            new Host("H4", 8080),
                            new Host("H5", 8080)

                    )), 0.5);

            reloader.run();
            Assert.assertTrue(reloader.isLoadedDataSuccessfullyRecently());
            Assert.assertEquals(
                    Arrays.asList(
                            new Host("H1", 8080),
                            new Host("H2", 8080),
                            new Host("H3", 8080),
                            new Host("H4", 8080),
                            new Host("H5", 8080)
                    ),
                    reloader.getHosts()
            );
        }

        {
            final CheckpointedHostsReloader reloader = new CheckpointedHostsReloader(hostsFile, new DummyHostsReloader(
                    Arrays.asList(
                            new Host("H1", 8080),
                            new Host("H2", 8080)
                    )), 0.5);

            reloader.run();
            Assert.assertFalse(reloader.isLoadedDataSuccessfullyRecently());
            Assert.assertEquals(
                    Arrays.asList(
                            new Host("H1", 8080),
                            new Host("H2", 8080),
                            new Host("H3", 8080),
                            new Host("H4", 8080),
                            new Host("H5", 8080)
                    ),
                    reloader.getHosts()
            );
        }

        {
            final CheckpointedHostsReloader reloader = new CheckpointedHostsReloader(hostsFile, new DummyHostsReloader(
                    Arrays.asList(
                            new Host("H1", 8080),
                            new Host("H2", 8080),
                            new Host("H3", 8080)
                    )), 0.5);

            reloader.run();
            Assert.assertTrue(reloader.isLoadedDataSuccessfullyRecently());
            Assert.assertEquals(
                    Arrays.asList(
                            new Host("H1", 8080),
                            new Host("H2", 8080),
                            new Host("H3", 8080)
                    ),
                    reloader.getHosts()
            );
        }
    }
}