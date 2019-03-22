package com.indeed.imhotep.fs;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.indeed.imhotep.client.Host;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
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
import static org.junit.Assert.fail;

/**
 * @author xweng
 */
public class P2PCachingPathTest {
    private static final TemporaryFolder tempDir = new TemporaryFolder();
    private static RemoteCachingFileSystem fileSystem;
    private static Path rootPath;

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
        new RemoteCachingFileSystemProvider().clearFileSystem();
    }

    @Test
    public void testInitialize() {
        final Host host = new Host("localhost", 1234);
        final P2PCachingPath path = new P2PCachingPath(fileSystem, "/var/imhotep", host);
        assertEquals("/remote/localhost:1234/var/imhotep", path.toString());

        final P2PCachingPath path2 = P2PCachingPath.newP2PCachingPath(fileSystem, "/remote/localhost:1234/var/imhotep");
        assertEquals( "/remote/localhost:1234/var/imhotep", path2.toString());

        final P2PCachingPath path3 = P2PCachingPath.newP2PCachingPath(fileSystem, "var/imhotep");
        assertEquals( "var/imhotep", path3.toString());

        try {
            P2PCachingPath.newP2PCachingPath(fileSystem, "/var/imhotep");
            fail("IllegalArgumentException is expected");
        } catch (final IllegalArgumentException e) {}
    }

    @Test
    public void testGetRoot() {
        final P2PCachingPath path = P2PCachingPath.newP2PCachingPath(fileSystem, "/remote/localhost:1234/var/imhotep");
        final Path rootPath = path.getRoot();
        assertTrue(rootPath instanceof P2PCachingPath);
        assertEquals("/remote/localhost:1234/", rootPath.toString());

        try {
            P2PCachingPath.newP2PCachingPath(fileSystem, "var/imhotep").getRoot();
            fail("IllegalArgumentException is expected");
        } catch (final IllegalArgumentException e) { }
    }

    @Test
    public void testGetFileName() {
        final P2PCachingPath absolutePath = P2PCachingPath.newP2PCachingPath(fileSystem, "/remote/localhost:1234/var/imhotep");
        final Path fileName = absolutePath.getFileName();
        assertTrue(fileName instanceof P2PCachingPath);
        assertEquals("imhotep", fileName.toString());

        final P2PCachingPath relativePath = P2PCachingPath.newP2PCachingPath(fileSystem, "var/imhotep");
        final Path relativeFileName = relativePath.getFileName();
        assertTrue(relativeFileName instanceof P2PCachingPath);
        assertEquals("imhotep", relativeFileName.toString());
    }

    @Test
    public void testGetParent() {
        final P2PCachingPath path = P2PCachingPath.newP2PCachingPath(fileSystem, "/remote/localhost:1234/var/imhotep");
        final Path parentPath = path.getParent();
        assertEquals("/remote/localhost:1234/var", parentPath.toString());
        assertNull(parentPath.getParent());

        final P2PCachingPath relativePath = P2PCachingPath.newP2PCachingPath(fileSystem, "var/imhotep");
        final Path relativeParentPath = relativePath.getParent();
        assertEquals("var", relativeParentPath.toString());
    }

    @Test
    public void testGetName() {
        final P2PCachingPath path = P2PCachingPath.newP2PCachingPath(fileSystem, "/remote/localhost:1234/var/imhotep");
        final Path absoluteName0 = path.getName(0);
        assertTrue(absoluteName0 instanceof P2PCachingPath);
        assertEquals("var", absoluteName0.toString());

        final Path absoluteName1 = path.getName(1);
        assertTrue(absoluteName1 instanceof P2PCachingPath);
        assertEquals("imhotep", absoluteName1.toString());

        final P2PCachingPath relativePath = P2PCachingPath.newP2PCachingPath(fileSystem, "var/imhotep");
        final Path relativeName0 = relativePath.getName(0);
        assertTrue(relativeName0 instanceof P2PCachingPath);
        assertEquals("var", relativeName0.toString());

        final Path relativeName1 = relativePath.getName(1);
        assertTrue(relativeName1 instanceof P2PCachingPath);
        assertEquals("imhotep", relativeName1.toString());
    }

    @Test
    public void testSubPath() {
        assertFalse(P2PCachingPath.newP2PCachingPath(fileSystem, "/remote/localhost:1234/var/imhotep").subpath(0, 1).isAbsolute());
        assertFalse(P2PCachingPath.newP2PCachingPath(fileSystem, "var/imhotep").subpath(0, 1).isAbsolute());

        assertEquals("var",
                P2PCachingPath.newP2PCachingPath(fileSystem, "/remote/localhost:1234/var/imhotep").subpath(0, 1).toString());
        assertEquals("var/imhotep",
                P2PCachingPath.newP2PCachingPath(fileSystem, "/remote/localhost:1234/var/imhotep").subpath(0, 2).toString());
        assertEquals("var",
                P2PCachingPath.newP2PCachingPath(fileSystem, "var/imhotep").subpath(0, 1).toString());
        assertEquals("var/imhotep",
                P2PCachingPath.newP2PCachingPath(fileSystem, "var/imhotep").subpath(0, 2).toString());

        try {
            P2PCachingPath.newP2PCachingPath(fileSystem, "/remote/localhost:1234/var/imhotep").subpath(0, 3);
            fail("IllegalArgumentException is expected");
        } catch (final IllegalArgumentException e) { }
    }

    @Test
    public void testNormalize() {
        assertEquals("/remote/localhost:1234/var/imhotep",
                P2PCachingPath.newP2PCachingPath(fileSystem, "/remote/localhost:1234/var/imhotep").normalize().toString());
        assertEquals("/remote/localhost:1234/var/imhotep",
                P2PCachingPath.newP2PCachingPath(fileSystem, "/remote/localhost:1234/var/imhotep/./").normalize().toString());
        assertEquals("/remote/localhost:1234/var",
                P2PCachingPath.newP2PCachingPath(fileSystem, "/remote/localhost:1234/var/imhotep/../").normalize().toString());
        assertEquals("/remote/localhost:1234/var",
                P2PCachingPath.newP2PCachingPath(fileSystem, "/remote/localhost:1234/var/imhotep/./../").normalize().toString());
        assertEquals("var/imhotep", P2PCachingPath.newP2PCachingPath(fileSystem, "var/imhotep").normalize().toString());
        assertEquals("var/imhotep", P2PCachingPath.newP2PCachingPath(fileSystem, "var/imhotep/./").normalize().toString());
        assertEquals("var", P2PCachingPath.newP2PCachingPath(fileSystem, "var/imhotep/../").normalize().toString());
        assertEquals("var", P2PCachingPath.newP2PCachingPath(fileSystem, "var/imhotep/./../").normalize().toString());
    }

    @Test
    public void testToUri() {
        assertEquals(URI.create("imhtpfs:/remote/localhost:1234/var/imhotep/"),
                P2PCachingPath.newP2PCachingPath(fileSystem, "/remote/localhost:1234/var/imhotep").toUri());
    }

    @Test
    public void testAsRelativePath() {
        assertEquals("var/imhotep",
                P2PCachingPath.newP2PCachingPath(fileSystem, "/remote/localhost:1234/var/imhotep").asRelativePath().toString());
        assertEquals("var/imhotep",
                P2PCachingPath.newP2PCachingPath(fileSystem, "var/imhotep").asRelativePath().toString());
    }

    @Test
    public void TestToP2PCachingPath() {
        assertEquals("/remote/localhost:1234/var/imhotep",
                P2PCachingPath.toP2PCachingPath(
                        (RemoteCachingPath) rootPath,
                        Paths.get("/var/imhotep"),
                        new Host("localhost", 1234)).toString());

        try {
            P2PCachingPath.toP2PCachingPath((RemoteCachingPath)rootPath, Paths.get("var/imhotep"), new Host("localhost", 1234));
            fail("IllegalArgumentException is expected");
        } catch (final IllegalArgumentException e) {}

        try {
            P2PCachingPath.toP2PCachingPath((RemoteCachingPath)rootPath, P2PCachingPath.newP2PCachingPath(fileSystem, "/remote/localhost:1234/var/imhotep"),
                    new Host("localhost", 1234));
            fail("IllegalArgumentException is expected");
        } catch (final IllegalArgumentException e) {}
    }
}