package com.indeed.imhotep.service;

import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.shardmaster.ShardMaster;
import com.indeed.imhotep.shardmaster.protobuf.AssignedShard;
import com.indeed.util.core.Pair;
import com.indeed.util.core.time.DefaultWallClock;
import com.indeed.util.core.time.StoppedClock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISOPeriodFormat;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author kenh
 */

public class ShardDirIteratorTest {
    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();

    private static final DateTimeFormatter SHARD_VERSION_FORMAT = DateTimeFormat.forPattern(".yyyyMMddHHmmss");
    private static final DateTimeZone TIME_ZONE = DateTimeZone.forOffsetHours(-6);
    private static final DateTime TODAY = DateTime.now(TIME_ZONE).withTimeAtStartOfDay();

    private static final Host LOCAL_HOST = new Host("localhost", 1230);

    private static Path shardOf(final Path datasetDir, final DateTime shardTime) {
        return datasetDir.resolve(ShardTimeUtils.toDailyShardPrefix(shardTime) + SHARD_VERSION_FORMAT.print(DateTime.now()));
    }

    private static ShardDir shardDirOf(final Path datasetDir, final DateTime shardTime) {
        return new ShardDir(shardOf(datasetDir, shardTime));
    }

    private static Path createShard(final Path datasetDir, final DateTime shardTime) throws IOException {
        final Path shardDir = shardOf(datasetDir, shardTime);
        Files.createDirectories(shardDir);
        return shardDir;
    }

    private static Path createShard(final Path datasetDir, final String shardId) throws IOException {
        final Path shardDir = datasetDir.resolve(shardId);
        Files.createDirectories(shardDir);
        return shardDir;
    }

    @Test
    public void testLocalShardIterator() throws IOException {
        final Path shardsDir = tempDir.newFolder("imhotep").toPath();

        final Set<Pair<String, ShardDir>> expected = new HashSet<>();

        final Path dataset1 = shardsDir.resolve("dataset1");
        for (int i = 0; i < 10; i++) {
            expected.add(Pair.of(dataset1.getFileName().toString(), new ShardDir(createShard(dataset1, TODAY.minusDays(i)))));
            createShard(dataset1, "SHARD" + i);
        }

        final Path dataset2 = shardsDir.resolve("dataset2");
        for (int i = 10; i < 100; i++) {
            expected.add(Pair.of(dataset2.getFileName().toString(), new ShardDir(createShard(dataset2, TODAY.minusDays(i)))));
            createShard(dataset2, "SHARD" + i);
        }

        final ShardDirIterator shardDirIterator = new ShardDirIteratorFactory(new DefaultWallClock(), null, LOCAL_HOST, null, null).get(shardsDir);
        Assert.assertTrue(shardDirIterator instanceof LocalShardDirIterator);
        Assert.assertEquals(
                expected,
                FluentIterable.from(shardDirIterator).toSet()
        );
    }

    private Path createConfig(final Map<String, Period> filterConfig) throws IOException {
        final Properties properties = new Properties();
        for (final Map.Entry<String, Period> entry : filterConfig.entrySet()) {
            properties.setProperty(FilteredShardDirIterator.Config.PROPERTY_PREFIX + entry.getKey(), entry.getValue().toString(ISOPeriodFormat.standard()));
        }
        final Path filterConfigPath = tempDir.newFile("filterConfig").toPath();
        properties.store(Files.newOutputStream(filterConfigPath), "Filter iterator config");
        return filterConfigPath;
    }

    @Test
    public void testTimeFilteredShardIterator() throws IOException {
        final Path shardsDir = tempDir.newFolder("imhotep").toPath();

        final Path dataset1 = shardsDir.resolve("dataset1");
        for (int i = 0; i < 20; i++) {
            createShard(dataset1, TODAY.minusDays(i));
            createShard(dataset1, "SHARD" + i);
        }

        final Path dataset2 = shardsDir.resolve("dataset2");
        for (int i = 10; i < 100; i++) {
            createShard(dataset2, TODAY.minusDays(i));
            createShard(dataset2, "SHARD" + i);
        }

        final Path dataset3 = shardsDir.resolve("dataset3");
        for (int i = 0; i < 100; i++) {
            createShard(dataset3, TODAY.minusDays(i));
            createShard(dataset3, "SHARD" + i);
        }

        final ShardDirIterator shardDirIterator = new ShardDirIteratorFactory(new StoppedClock(TODAY.getMillis()),
                null,
                LOCAL_HOST,
                createConfig(
                        ImmutableMap.<String, Period>builder()
                                .put("dataset1", Period.days(5))
                                .put("dataset2", Period.days(20))
                                .put("dataset4", Period.days(30))
                                .build()
                ).toString(), null).get(shardsDir);
        Assert.assertTrue(shardDirIterator instanceof FilteredShardDirIterator);
        Assert.assertEquals(
                ImmutableSet.builder()
                        .add(Pair.of("dataset1", shardDirOf(dataset1, TODAY.minusDays(0))))
                        .add(Pair.of("dataset1", shardDirOf(dataset1, TODAY.minusDays(1))))
                        .add(Pair.of("dataset1", shardDirOf(dataset1, TODAY.minusDays(2))))
                        .add(Pair.of("dataset1", shardDirOf(dataset1, TODAY.minusDays(3))))
                        .add(Pair.of("dataset1", shardDirOf(dataset1, TODAY.minusDays(4))))
                        .add(Pair.of("dataset1", shardDirOf(dataset1, TODAY.minusDays(5))))

                        .add(Pair.of("dataset2", shardDirOf(dataset2, TODAY.minusDays(10))))
                        .add(Pair.of("dataset2", shardDirOf(dataset2, TODAY.minusDays(11))))
                        .add(Pair.of("dataset2", shardDirOf(dataset2, TODAY.minusDays(12))))
                        .add(Pair.of("dataset2", shardDirOf(dataset2, TODAY.minusDays(13))))
                        .add(Pair.of("dataset2", shardDirOf(dataset2, TODAY.minusDays(14))))
                        .add(Pair.of("dataset2", shardDirOf(dataset2, TODAY.minusDays(15))))
                        .add(Pair.of("dataset2", shardDirOf(dataset2, TODAY.minusDays(16))))
                        .add(Pair.of("dataset2", shardDirOf(dataset2, TODAY.minusDays(17))))
                        .add(Pair.of("dataset2", shardDirOf(dataset2, TODAY.minusDays(18))))
                        .add(Pair.of("dataset2", shardDirOf(dataset2, TODAY.minusDays(19))))
                        .add(Pair.of("dataset2", shardDirOf(dataset2, TODAY.minusDays(20))))
                        .build(),
                FluentIterable.from(shardDirIterator).toSet()
        );
    }

    private static AssignedShard assignedShard(final Path datasetDir, final DateTime shardTime) {
        final ShardDir shardDir = shardDirOf(datasetDir, shardTime);
        return AssignedShard.newBuilder()
                .setDataset(shardDir.getIndexDir().getParent().getFileName().toString())
                .setShardPath(shardDir.getIndexDir().toString())
                .build();
    }

    @Test
    public void testShardMasterShardIterator() throws IOException {
        final Path shardsDir = tempDir.newFolder("imhotep").toPath();

        final Path dataset1 = shardsDir.resolve("dataset1");
        for (int i = 0; i < 20; i++) {
            createShard(dataset1, TODAY.minusDays(i));
        }

        final Path dataset2 = shardsDir.resolve("dataset2");
        for (int i = 10; i < 100; i++) {
            createShard(dataset2, TODAY.minusDays(i));
        }

        final Path dataset3 = shardsDir.resolve("dataset3");
        for (int i = 0; i < 100; i++) {
            createShard(dataset3, TODAY.minusDays(i));
        }

        final ShardMaster shardMaster = new ShardMaster() {
            @Override
            public Iterable<AssignedShard> getAssignments(final Host node) throws IOException {
                return Arrays.asList(
                        assignedShard(dataset1, TODAY.minusDays(1)),
                        assignedShard(dataset1, TODAY.minusDays(3)),
                        assignedShard(dataset1, TODAY.minusDays(5)),
                        assignedShard(dataset1, TODAY.minusDays(7)),
                        assignedShard(dataset1, TODAY.minusDays(11)),
                        assignedShard(dataset1, TODAY.minusDays(13)),
                        assignedShard(dataset1, TODAY.minusDays(17)),
                        assignedShard(dataset1, TODAY.minusDays(19)),

                        assignedShard(dataset2, TODAY.minusDays(20)),
                        assignedShard(dataset2, TODAY.minusDays(30)),
                        assignedShard(dataset2, TODAY.minusDays(50))
                );
            }
        };

        final ShardDirIterator shardDirIterator = new ShardDirIteratorFactory(new StoppedClock(TODAY.getMillis()),
                Suppliers.ofInstance(shardMaster),
                LOCAL_HOST,
                null, Boolean.TRUE.toString()).get(shardsDir);

        Assert.assertTrue(shardDirIterator instanceof ShardMasterShardDirIterator);
        Assert.assertEquals(
                ImmutableSet.builder()
                        .add(Pair.of("dataset1", shardDirOf(dataset1, TODAY.minusDays(1))))
                        .add(Pair.of("dataset1", shardDirOf(dataset1, TODAY.minusDays(3))))
                        .add(Pair.of("dataset1", shardDirOf(dataset1, TODAY.minusDays(5))))
                        .add(Pair.of("dataset1", shardDirOf(dataset1, TODAY.minusDays(7))))
                        .add(Pair.of("dataset1", shardDirOf(dataset1, TODAY.minusDays(11))))
                        .add(Pair.of("dataset1", shardDirOf(dataset1, TODAY.minusDays(13))))
                        .add(Pair.of("dataset1", shardDirOf(dataset1, TODAY.minusDays(17))))
                        .add(Pair.of("dataset1", shardDirOf(dataset1, TODAY.minusDays(19))))

                        .add(Pair.of("dataset2", shardDirOf(dataset2, TODAY.minusDays(20))))
                        .add(Pair.of("dataset2", shardDirOf(dataset2, TODAY.minusDays(30))))
                        .add(Pair.of("dataset2", shardDirOf(dataset2, TODAY.minusDays(50))))
                        .build(),
                FluentIterable.from(shardDirIterator).toSet()
        );
    }
}