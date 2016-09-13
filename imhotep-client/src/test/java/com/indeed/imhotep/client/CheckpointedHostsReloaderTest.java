package com.indeed.imhotep.client;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author kenh
 */

public class CheckpointedHostsReloaderTest {
    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testLoad() throws IOException {
        final File hostsFile = new File(tempDir.getRoot(), "hosts.dat");
        // loading no results should result in failure
        {
            final CheckpointedHostsReloader reloader = new CheckpointedHostsReloader(hostsFile, new DummyHostsReloader(
                    Collections.<Host>emptyList()), 0.5);
            reloader.run();
            Assert.assertFalse(reloader.isLoadedDataSuccessfullyRecently());
        }

        // loading some initial data should succeed
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

        // fluctuation down should result in load failure
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

        // fluctuation above threshold is okay
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

    List<Host> hosts = new ArrayList<>();

    @Test
    public void testGradualDecrease() throws IOException {
        final File hostsFile = new File(tempDir.getRoot(), "hosts.dat");

        final HostsReloader underlyingReloader = new HostsReloader() {
            @Override
            public List<Host> getHosts() {
                return hosts;
            }

            @Override
            public boolean isLoadedDataSuccessfullyRecently() {
                return true;
            }

            @Override
            public void shutdown() {
            }

            @Override
            public void run() {
            }
        };

        {
            hosts = Arrays.asList(
                    new Host("H1", 8080),
                    new Host("H2", 8080),
                    new Host("H3", 8080),
                    new Host("H4", 8080),
                    new Host("H5", 8080)
            );

            final CheckpointedHostsReloader reloader = new CheckpointedHostsReloader(hostsFile, underlyingReloader, 0.5);

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

            hosts = Arrays.asList(
                    new Host("H1", 8080),
                    new Host("H2", 8080),
                    new Host("H3", 8080),
                    new Host("H4", 8080),
                    new Host("H5", 8080),
                    new Host("H6", 8080),
                    new Host("H7", 8080),
                    new Host("H8", 8080),
                    new Host("H9", 8080),
                    new Host("H10", 8080)
            );

            reloader.run();
            Assert.assertTrue(reloader.isLoadedDataSuccessfullyRecently());
            Assert.assertEquals(
                    Arrays.asList(
                            new Host("H1", 8080),
                            new Host("H2", 8080),
                            new Host("H3", 8080),
                            new Host("H4", 8080),
                            new Host("H5", 8080),
                            new Host("H6", 8080),
                            new Host("H7", 8080),
                            new Host("H8", 8080),
                            new Host("H9", 8080),
                            new Host("H10", 8080)
                    ),
                    reloader.getHosts()
            );

            hosts = Arrays.asList(
                    new Host("H1", 8080),
                    new Host("H2", 8080),
                    new Host("H3", 8080),
                    new Host("H4", 8080),
                    new Host("H5", 8080),
                    new Host("H6", 8080),
                    new Host("H7", 8080),
                    new Host("H8", 8080)
            );

            reloader.run();
            Assert.assertTrue(reloader.isLoadedDataSuccessfullyRecently());
            Assert.assertEquals(
                    Arrays.asList(
                            new Host("H1", 8080),
                            new Host("H2", 8080),
                            new Host("H3", 8080),
                            new Host("H4", 8080),
                            new Host("H5", 8080),
                            new Host("H6", 8080),
                            new Host("H7", 8080),
                            new Host("H8", 8080)
                    ),
                    reloader.getHosts()
            );

            hosts = Arrays.asList(
                    new Host("H1", 8080),
                    new Host("H2", 8080),
                    new Host("H3", 8080),
                    new Host("H4", 8080),
                    new Host("H5", 8080),
                    new Host("H6", 8080)
            );

            reloader.run();
            Assert.assertTrue(reloader.isLoadedDataSuccessfullyRecently());
            Assert.assertEquals(
                    Arrays.asList(
                            new Host("H1", 8080),
                            new Host("H2", 8080),
                            new Host("H3", 8080),
                            new Host("H4", 8080),
                            new Host("H5", 8080),
                            new Host("H6", 8080)
                    ),
                    reloader.getHosts()
            );


            hosts = Arrays.asList(
                    new Host("H1", 8080),
                    new Host("H2", 8080),
                    new Host("H3", 8080),
                    new Host("H4", 8080)
            );

            reloader.run();
            Assert.assertFalse(reloader.isLoadedDataSuccessfullyRecently());
            Assert.assertEquals(
                    Arrays.asList(
                            new Host("H1", 8080),
                            new Host("H2", 8080),
                            new Host("H3", 8080),
                            new Host("H4", 8080),
                            new Host("H5", 8080),
                            new Host("H6", 8080)
                    ),
                    reloader.getHosts()
            );

            hosts = Arrays.asList(
                    new Host("H1", 8080),
                    new Host("H2", 8080),
                    new Host("H3", 8080),
                    new Host("H4", 8080),
                    new Host("H5", 8080)
            );

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
    }
}