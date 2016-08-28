package com.indeed.imhotep.fs.sql;

import com.google.common.collect.Lists;
import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;
import com.indeed.imhotep.dbutil.DbDataFixture;
import com.indeed.imhotep.fs.RemoteFileMetadata;
import com.indeed.imhotep.fs.db.metadata.Tables;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

/**
 * @author kenh
 */

public class FileMetadataDaoTest {
    @Rule
    public final DbDataFixture dbDataFixture = new DbDataFixture(Collections.singletonList(Tables.TBLFILEMETADATA));
    private FileMetadataDao fileMetadataDao;
    private DateTime now = DateTime.now();

    @Before
    public void setUp() throws IOException, SQLException, URISyntaxException {
        fileMetadataDao = new FileMetadataDao(dbDataFixture.getDataSource());
    }

    @Test
    public void testGetMetaAndListDir() throws SQLException {
        Assert.assertNull(fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), ""));
        Assert.assertNull(fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), "d"));
        Assert.assertNull(fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), "d/e"));

        final RemoteFileMetadata dirmeta_base = new RemoteFileMetadata("");

        final RemoteFileMetadata dirmeta_d = new RemoteFileMetadata("d");

        final RemoteFileMetadata filemeta_de1 = new RemoteFileMetadata(new FileMetadata(
                "d/e1",
                1000,
                now.getMillis(),
                "cf15de77e7e1469de28b83e46ed5ef1a",
                0L,
                SquallArchiveCompressor.GZIP,
                "archive0.bin"),
                100
        );

        final RemoteFileMetadata filemeta_de2 = new RemoteFileMetadata(new FileMetadata(
                "d/e2",
                2000,
                now.getMillis(),
                "f043e6b2c7b57858d70546153e9d7c15",
                0,
                SquallArchiveCompressor.GZIP,
                "archive0.bin"),
                200
        );

        final RemoteFileMetadata dirmeta_dd = new RemoteFileMetadata("d/d");

        final RemoteFileMetadata filemeta_dde1 = new RemoteFileMetadata(new FileMetadata(
                "d/d/e1",
                3000,
                now.getMillis(),
                "c59548c3c576228486a1f0037eb16a1b",
                0,
                SquallArchiveCompressor.GZIP,
                "archive0.bin"),
                300
        );

        final RemoteFileMetadata filemeta_dde2 = new RemoteFileMetadata(new FileMetadata(
                "d/d/e2",
                4000,
                now.getMillis(),
                "20fc93f9da1afbbc5fec61775989869e",
                0,
                SquallArchiveCompressor.GZIP,
                "archive0.bin"),
                400
        );

        Assert.assertFalse(fileMetadataDao.hasShard(Paths.get("a/b/c")));

        fileMetadataDao.cacheMetadata(Paths.get("a/b/c/"), Arrays.asList(
                dirmeta_base,
                new RemoteFileMetadata("d/"), // intentionally with trailing sep
                filemeta_de1,
                filemeta_de2,
                dirmeta_dd,
                filemeta_dde1,
                filemeta_dde2
        ));

        Assert.assertTrue(fileMetadataDao.hasShard(Paths.get("a/b/c")));
        Assert.assertTrue(fileMetadataDao.hasShard(Paths.get("a/b/c/")));

        final RemoteFileMetadata dirmeta_def = new RemoteFileMetadata("def");
        fileMetadataDao.cacheMetadata(Paths.get("a/b/c/"), Collections.singletonList(
                dirmeta_def
        ));

        Assert.assertTrue(fileMetadataDao.hasShard(Paths.get("a/b/c")));

        Assert.assertEquals(dirmeta_d, fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), "d"));
        Assert.assertEquals(dirmeta_d, fileMetadataDao.getFileMetadata(Paths.get("a/b/c/"), "d"));
        Assert.assertNull(fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), "d/e1/"));
        Assert.assertEquals(filemeta_de1, fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), "d/e1"));
        Assert.assertEquals(filemeta_de2, fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), "d/e2"));
        Assert.assertNull(fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), "d/e3"));

        Assert.assertEquals(
                Arrays.asList(
                        dirmeta_d,
                        dirmeta_def
                ),
                Lists.newArrayList(fileMetadataDao.listDirectory(Paths.get("a/b/c"), "")));

        Assert.assertEquals(
                Arrays.asList(
                        dirmeta_dd,
                        filemeta_de1,
                        filemeta_de2
                ),
                Lists.newArrayList(fileMetadataDao.listDirectory(Paths.get("a/b/c"), "d")));

        Assert.assertEquals(dirmeta_dd, fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), "d/d"));
        Assert.assertEquals(dirmeta_dd, fileMetadataDao.getFileMetadata(Paths.get("a/b/c/"), "d/d"));
        Assert.assertNull(fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), "d/d/e1/"));
        Assert.assertEquals(filemeta_dde1, fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), "d/d/e1"));
        Assert.assertEquals(filemeta_dde2, fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), "d/d/e2"));
        Assert.assertNull(fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), "d/d/e3"));

        Assert.assertEquals(
                Arrays.asList(
                        filemeta_dde1,
                        filemeta_dde2
                ),
                Lists.newArrayList(fileMetadataDao.listDirectory(Paths.get("a/b/c"), "d/d")));

        Assert.assertEquals(dirmeta_def, fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), "def"));
        Assert.assertEquals(
                Collections.emptyList(),
                Lists.newArrayList(fileMetadataDao.listDirectory(Paths.get("a/b/c/"), "def"))
        );

        Assert.assertNull(fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), "d/d/d"));
        Assert.assertNull(fileMetadataDao.getFileMetadata(Paths.get("a/b/c/d"), "d"));

        Assert.assertFalse(fileMetadataDao.hasShard(Paths.get("a/b")));
        Assert.assertFalse(fileMetadataDao.hasShard(Paths.get("a/b/c/d")));
    }

    @Test
    public void testDuplicateCache() throws SQLException {
        Assert.assertNull(fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), ""));
        Assert.assertNull(fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), "d"));
        Assert.assertNull(fileMetadataDao.getFileMetadata(Paths.get("a/b/c"), "d/e"));

        final RemoteFileMetadata dirmeta_base = new RemoteFileMetadata("");

        final RemoteFileMetadata dirmeta_d = new RemoteFileMetadata("d");

        final RemoteFileMetadata filemeta_de1 = new RemoteFileMetadata(new FileMetadata(
                "d/e1",
                1000,
                now.getMillis(),
                "cf15de77e7e1469de28b83e46ed5ef1a",
                0,
                SquallArchiveCompressor.GZIP,
                "archive0.bin"),
                100
        );

        final RemoteFileMetadata filemeta_de2 = new RemoteFileMetadata(new FileMetadata(
                "d/e2",
                2000,
                now.getMillis(),
                "f043e6b2c7b57858d70546153e9d7c15",
                0,
                SquallArchiveCompressor.GZIP,
                "archive0.bin"),
                200
        );

        final RemoteFileMetadata dirmeta_dd = new RemoteFileMetadata("d/d");

        final RemoteFileMetadata filemeta_dde1 = new RemoteFileMetadata(new FileMetadata(
                "d/d/e1",
                3000,
                now.getMillis(),
                "c59548c3c576228486a1f0037eb16a1b",
                0,
                SquallArchiveCompressor.GZIP,
                "archive0.bin"),
                300
        );

        final RemoteFileMetadata filemeta_dde2 = new RemoteFileMetadata(new FileMetadata(
                "d/d/e2",
                4000,
                now.getMillis(),
                "20fc93f9da1afbbc5fec61775989869e",
                0,
                SquallArchiveCompressor.GZIP,
                "archive0.bin"),
                400
        );

        fileMetadataDao.cacheMetadata(Paths.get("a/b/c/"), Arrays.asList(
                dirmeta_base,
                dirmeta_d,
                filemeta_de1,
                filemeta_de2,
                dirmeta_dd,
                filemeta_dde1,
                filemeta_dde2
        ));

        Assert.assertEquals(
                Arrays.asList(
                        dirmeta_dd,
                        filemeta_de1,
                        filemeta_de2
                ),
                Lists.newArrayList(fileMetadataDao.listDirectory(Paths.get("a/b/c"), "d")));

        final RemoteFileMetadata filemeta_de2_2 = new RemoteFileMetadata(new FileMetadata(
                "d/e2",
                2500,
                now.getMillis(),
                "f043e6b2c7b57858d70546153e9d7c15",
                0,
                SquallArchiveCompressor.GZIP,
                "archive0.bin"),
                250
        );

        final RemoteFileMetadata filemeta_de3 = new RemoteFileMetadata(new FileMetadata(
                "d/e3",
                3500,
                now.getMillis(),
                "ea182a576dd179c0dd56abe534ca1f49",
                0,
                SquallArchiveCompressor.GZIP,
                "archive0.bin"),
                350
        );

        fileMetadataDao.cacheMetadata(Paths.get("a/b/c/"), Arrays.asList(
                dirmeta_base,
                dirmeta_d,
                filemeta_de1,
                filemeta_de2_2,
                filemeta_de3,
                dirmeta_dd,
                filemeta_dde1,
                filemeta_dde2
        ));

        Assert.assertEquals(
                Arrays.asList(
                        dirmeta_dd,
                        filemeta_de1,
                        filemeta_de2_2,
                        filemeta_de3
                ),
                Lists.newArrayList(fileMetadataDao.listDirectory(Paths.get("a/b/c"), "d")));
    }

    @Test
    public void testCacheFromDifferentShards() throws SQLException {
        final RemoteFileMetadata dirmeta_1a = new RemoteFileMetadata("a");
        final RemoteFileMetadata filemeta_1ab = new RemoteFileMetadata(new FileMetadata(
                "a/b",
                3000,
                now.getMillis(),
                "0254f24a108bfd61c1722ed349debb6e",
                0,
                SquallArchiveCompressor.GZIP,
                "archive0.bin"),
                300
        );

        fileMetadataDao.cacheMetadata(Paths.get("1"), Arrays.asList(dirmeta_1a, filemeta_1ab));

        final RemoteFileMetadata dirmeta_2a = new RemoteFileMetadata("a");
        final RemoteFileMetadata filemeta_2ab = new RemoteFileMetadata(new FileMetadata(
                "a/b",
                2000,
                now.getMillis(),
                "d92f04e0cef20c61d2241b21ea3d94ab",
                0,
                SquallArchiveCompressor.GZIP,
                "archive0.bin"),
                200
        );
        final RemoteFileMetadata filemeta_2ac = new RemoteFileMetadata(new FileMetadata(
                "a/c",
                2500,
                now.getMillis(),
                "88d79e92916f4271f4e1113d0f22d532",
                0,
                SquallArchiveCompressor.GZIP,
                "archive0.bin"),
                250
        );

        fileMetadataDao.cacheMetadata(Paths.get("2"), Arrays.asList(dirmeta_2a, filemeta_2ab, filemeta_2ac));

        Assert.assertEquals(
                Arrays.asList(
                        filemeta_1ab
                ), Lists.newArrayList(fileMetadataDao.listDirectory(Paths.get("1"), "a")));

        Assert.assertEquals(
                Arrays.asList(
                        filemeta_2ab,
                        filemeta_2ac
                ), Lists.newArrayList(fileMetadataDao.listDirectory(Paths.get("2"), "a")));
    }
}