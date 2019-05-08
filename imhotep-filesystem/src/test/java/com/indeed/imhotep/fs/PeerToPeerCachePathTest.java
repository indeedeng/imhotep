package com.indeed.imhotep.fs;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.indeed.imhotep.client.Host;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author xweng
 */
public class PeerToPeerCachePathTest {
    private static final TemporaryFolder tempDir = new TemporaryFolder();
    private static RemoteCachingFileSystem fileSystem;
    private static Path rootPath;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws IOException  {
        tempDir.create();
        final File localStoreDir = tempDir.newFolder("local-store");

        final Properties properties = new Properties();
        properties.putAll(
                ImmutableMap.<String, String>builder()
                        // cache
                        .put("imhotep.fs.cache.root.uri", tempDir.newFolder("cache").toURI().toString())
                        // p2pcache
                        .put("imhotep.fs.p2p.cache.root.uri", tempDir.newFolder("p2pcache").toURI().toString())
                        .put("imhotep.fs.p2p.cache.enable", "true")
                        .put("imhotep.fs.enabled", "true")
                        // local
                        .put("imhotep.fs.filestore.local.root.uri", localStoreDir.toURI().toString())
                        // hdfs
                        .put("imhotep.fs.filestore.hdfs.root.uri", localStoreDir.toURI().toString())
                        .put("imhotep.fs.sqar.metadata.cache.path", new File(tempDir.newFolder("sqardb"), "lsmtree").toString())
                        .put("imhotep.fs.store.type", "local")
                        .put("imhotep.fs.cache.size.gb", "1")
                        .put("imhotep.fs.cache.block.size.bytes", "4096")
                        .put("imhotep.fs.p2p.cache.size.gb", "1")
                        .put("imhotep.fs.p2p.cache.block.size.bytes", "4096")
                        .build()
        );

        final File tempConfigFile = tempDir.newFile("imhotep-daemon-test-filesystem-config.properties");
        properties.store(new FileOutputStream(tempConfigFile.getPath()), null);

        fileSystem = (RemoteCachingFileSystem) RemoteCachingFileSystemProvider.newFileSystem(tempConfigFile);
        rootPath = Iterables.getFirst(fileSystem.getRootDirectories(), null);
    }

    @AfterClass
    public static void tearDown() {
        RemoteCachingFileSystemProvider.closeFileSystem();
    }

    @Test
    public void testInitialize() {
        final PeerToPeerCachePath path2 = PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/localhost:1234/var/imhotep");
        assertEquals( "/remote/localhost:1234/var/imhotep", path2.toString());

        final PeerToPeerCachePath path3 = PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "var/imhotep");
        assertEquals( "var/imhotep", path3.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitializeAbsolutePathWithoutHost() {
        PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/var/imhotep");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitializeWithExtraBackSlash() {
        PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/local\\host:1234/var/imhotep");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitializeWithExtraColon() {
        PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/local:host:1234/var/imhotep");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitializeWithExtraSlash() {
        PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/local/host:1234/var/imhotep");
    }

    @Test
    public void testGetRoot() {
        final PeerToPeerCachePath path = PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/localhost:1234/var/imhotep");
        final Path rootPath = path.getRoot();
        assertTrue(rootPath instanceof PeerToPeerCachePath);
        assertEquals("/remote/localhost:1234/", rootPath.toString());

        thrown.expect(IllegalArgumentException.class);
        PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "var/imhotep").getRoot();
    }

    @Test
    public void testGetFileName() {
        final PeerToPeerCachePath absolutePath = PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/localhost:1234/var/imhotep");
        final Path fileName = absolutePath.getFileName();
        assertTrue(fileName instanceof PeerToPeerCachePath);
        assertEquals("imhotep", fileName.toString());

        final PeerToPeerCachePath relativePath = PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "var/imhotep");
        final Path relativeFileName = relativePath.getFileName();
        assertTrue(relativeFileName instanceof PeerToPeerCachePath);
        assertEquals("imhotep", relativeFileName.toString());
    }

    @Test
    public void testGetParent() {
        final PeerToPeerCachePath path = PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/localhost:1234/var/imhotep");
        final Path parentPath = path.getParent();
        assertEquals("/remote/localhost:1234/var", parentPath.toString());
        assertNull(parentPath.getParent());

        final PeerToPeerCachePath relativePath = PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "var/imhotep");
        final Path relativeParentPath = relativePath.getParent();
        assertEquals("var", relativeParentPath.toString());
    }

    @Test
    public void testGetName() {
        final PeerToPeerCachePath path = PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/localhost:1234/var/imhotep");
        final Path absoluteName0 = path.getName(0);
        assertTrue(absoluteName0 instanceof PeerToPeerCachePath);
        assertEquals("var", absoluteName0.toString());

        final Path absoluteName1 = path.getName(1);
        assertTrue(absoluteName1 instanceof PeerToPeerCachePath);
        assertEquals("imhotep", absoluteName1.toString());

        final PeerToPeerCachePath relativePath = PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "var/imhotep");
        final Path relativeName0 = relativePath.getName(0);
        assertTrue(relativeName0 instanceof PeerToPeerCachePath);
        assertEquals("var", relativeName0.toString());

        final Path relativeName1 = relativePath.getName(1);
        assertTrue(relativeName1 instanceof PeerToPeerCachePath);
        assertEquals("imhotep", relativeName1.toString());
    }

    @Test
    public void testSubPath() {
        assertFalse(PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/localhost:1234/var/imhotep").subpath(0, 1).isAbsolute());
        assertFalse(PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "var/imhotep").subpath(0, 1).isAbsolute());

        assertEquals("var",
                PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/localhost:1234/var/imhotep").subpath(0, 1).toString());
        assertEquals("var/imhotep",
                PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/localhost:1234/var/imhotep").subpath(0, 2).toString());
        assertEquals("var",
                PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "var/imhotep").subpath(0, 1).toString());
        assertEquals("var/imhotep",
                PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "var/imhotep").subpath(0, 2).toString());

        thrown.expect(IllegalArgumentException.class);
        PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/localhost:1234/var/imhotep").subpath(0, 3);
    }

    @Test
    public void testNormalize() {
        assertEquals("/remote/localhost:1234/var/imhotep",
                PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/localhost:1234/var/imhotep").normalize().toString());
        assertEquals("/remote/localhost:1234/var/imhotep",
                PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/localhost:1234/var/imhotep/./").normalize().toString());
        assertEquals("/remote/localhost:1234/var",
                PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/localhost:1234/var/imhotep/../").normalize().toString());
        assertEquals("/remote/localhost:1234/var",
                PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/localhost:1234/var/imhotep/./../").normalize().toString());
        assertEquals("var/imhotep", PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "var/imhotep").normalize().toString());
        assertEquals("var/imhotep", PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "var/imhotep/./").normalize().toString());
        assertEquals("var", PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "var/imhotep/../").normalize().toString());
        assertEquals("var", PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "var/imhotep/./../").normalize().toString());
    }

    @Test
    public void testToUri() {
        assertEquals(URI.create("imhtpfs:/remote/localhost:1234/var/imhotep/"),
                PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/localhost:1234/var/imhotep").toUri());
    }

    @Test
    public void testAsRelativePath() {
        assertEquals("var/imhotep",
                PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/localhost:1234/var/imhotep").asRelativePath().toString());
        assertEquals("var/imhotep",
                PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "var/imhotep").asRelativePath().toString());
    }

    @Test
    public void TestToPeerToPeerCachePath() {
        assertEquals("/remote/localhost:1234/var/imhotep",
                PeerToPeerCachePath.toPeerToPeerCachePath(
                        (RemoteCachingPath) rootPath,
                        Paths.get("/var/imhotep"),
                        new Host("localhost", 1234)).toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void TestToPeerToPeerCachePathWithRelativePath() {
        PeerToPeerCachePath.toPeerToPeerCachePath((RemoteCachingPath)rootPath, Paths.get("var/imhotep"), new Host("localhost", 1234));
    }

    @Test(expected = IllegalArgumentException.class)
    public void TestToPeerToPeerCachePathNonRemoteCachingPath() {
        PeerToPeerCachePath.toPeerToPeerCachePath((RemoteCachingPath)rootPath, PeerToPeerCachePath.newPeerToPeerCachePath(fileSystem, "/remote/localhost:1234/var/imhotep"),
                new Host("localhost", 1234));
    }
}