package com.indeed.imhotep;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.RegEx;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author michihiko
 */
public final class DynamicIndexSubshardDirnameUtil {
    private static final DateTimeZone ZONE = DateTimeZone.forOffsetHours(-6);
    private static final DateTimeFormatter YYYYMMDDHH = DateTimeFormat.forPattern("yyyyMMdd.HH").withZone(ZONE);

    private enum DynamicIndexPattern {
        START_TIME("startTime", "\\d{8}\\.\\d{2}"),
        END_TIME("endTime", "\\d{8}\\.\\d{2}"),
        SUBSHARD_ID("subshardId", "\\d{1,9}"),
        NUM_SUBSHARDS("numSubshards", "\\d{1,9}"),
        UPDATE_ID("updateId", "\\d{1,18}"),
        TIMESTAMP("timestamp", "\\d{1,18}"),
        SHARD_ID("shardId", "dindex" + START_TIME.getRegex() + "-" + END_TIME.getRegex() + "\\." + SUBSHARD_ID.getRegex() + "\\." + NUM_SUBSHARDS.getRegex()),
        INDEX_NAME("indexName", SHARD_ID.getRegex() + "\\." + UPDATE_ID.getRegex() + "\\." + TIMESTAMP.getRegex());

        private final String name;
        private final String regex;
        private final Pattern compiled;

        DynamicIndexPattern(final String name, @RegEx final String regex) {
            this.name = name;
            this.regex = regex;
            this.compiled = Pattern.compile("^" + getRegex() + "$");
        }

        public String getRegex() {
            return String.format("(?<%s>%s)", name, regex);
        }

        public Matcher matcher(final String string) {
            return this.compiled.matcher(string);
        }

        public String getGroupIn(final Matcher matcher) {
            return matcher.group(name);
        }
    }

    private DynamicIndexSubshardDirnameUtil() {
    }

    public static String getShardId(final long startTimeMillis, final long endTimeMillis, final int subshardId, final int numSubshards) {
        return "dindex"
                + YYYYMMDDHH.print(startTimeMillis)
                + '-'
                + YYYYMMDDHH.print(endTimeMillis)
                + '.'
                + subshardId
                + '.'
                + numSubshards;
    }

    public static class DynamicIndexShardInfo implements Comparable<DynamicIndexShardInfo> {
        private final String name;
        private final String id;
        private final long updateId;
        private final int subshardId;
        private final int numSubshards;
        private final long timestamp;

        private DynamicIndexShardInfo(@Nonnull final String name) throws IllegalArgumentException {
            this.name = name;
            final Matcher dynamicMatcher = DynamicIndexPattern.INDEX_NAME.matcher(name);
            if (dynamicMatcher.matches()) {
                this.id = DynamicIndexPattern.SHARD_ID.getGroupIn(dynamicMatcher);
                this.updateId = Long.parseLong(DynamicIndexPattern.UPDATE_ID.getGroupIn(dynamicMatcher));
                this.timestamp = Long.parseLong(DynamicIndexPattern.TIMESTAMP.getGroupIn(dynamicMatcher));
                this.subshardId = Integer.parseInt(DynamicIndexPattern.SUBSHARD_ID.getGroupIn(dynamicMatcher));
                this.numSubshards = Integer.parseInt(DynamicIndexPattern.NUM_SUBSHARDS.getGroupIn(dynamicMatcher));
            } else {
                throw new IllegalArgumentException(this.name + " is invalid name for dynamic index");
            }
        }

        @Override
        public int compareTo(@Nonnull final DynamicIndexShardInfo o) {
            return ComparisonChain.start()
                    .compare(id, o.id)
                    .compare(updateId, o.updateId)
                    .compare(timestamp, o.timestamp)
                    .result();
        }

        @Override
        public boolean equals(@Nullable final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DynamicIndexShardInfo)) {
                return false;
            }
            final DynamicIndexShardInfo that = (DynamicIndexShardInfo) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        public String getName() {
            return name;
        }

        public String getId() {
            return id;
        }

        public int getSubshardId() {
            return subshardId;
        }

        public int getNumSubshards() {
            return numSubshards;
        }

        public long getUpdateId() {
            return updateId;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    public static DynamicIndexShardInfo parse(final String name) {
        return new DynamicIndexShardInfo(name);
    }

    public static Optional<DynamicIndexShardInfo> tryParse(final String name) {
        try {
            return Optional.of(parse(name));
        } catch (final IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    public static boolean isValidDynamicIndexName(final String name) {
        return DynamicIndexPattern.INDEX_NAME.matcher(name).matches();
    }

    public static boolean isValidDynamicShardId(final String shardId) {
        return DynamicIndexPattern.SHARD_ID.matcher(shardId).matches();
    }

    public static DateTime parseStartTimeFromShardId(final String shardId) {
        final Matcher matcher = DynamicIndexPattern.SHARD_ID.matcher(shardId);
        Preconditions.checkArgument(matcher.matches());
        return YYYYMMDDHH.parseDateTime(DynamicIndexPattern.START_TIME.getGroupIn(matcher));
    }

    public static Interval parseTimeRangeFromShardId(final String shardId) {
        final Matcher matcher = DynamicIndexPattern.SHARD_ID.matcher(shardId);
        Preconditions.checkArgument(matcher.matches());
        final DateTime start = YYYYMMDDHH.parseDateTime(DynamicIndexPattern.START_TIME.getGroupIn(matcher));
        final DateTime end = YYYYMMDDHH.parseDateTime(DynamicIndexPattern.END_TIME.getGroupIn(matcher));
        return new Interval(start, end);
    }

    public static int parseSubshardIdFromShardId(final String shardId) {
        final Matcher matcher = DynamicIndexPattern.SHARD_ID.matcher(shardId);
        Preconditions.checkArgument(matcher.matches());
        return Integer.parseInt(DynamicIndexPattern.SUBSHARD_ID.getGroupIn(matcher));
    }

    /**
     * For iterable of T, where each element corresponds to a subshard, select the element that corresponding to the latest subshard.
     *
     * @param shardId       Id of the subshard to search, e.g. dindex20170705.02-20170705.03.1.4
     * @param candidates    Iterable of T, e.g. List&lt;Path&gt;. nulls are ignored.
     * @param dirNameGetter Converter from T to the name of the subshard, e.g. (path) -&gt; path.getFileName().toString(). Ignored if the return value is null.
     */
    public static <T> Optional<T> selectLatest(final String shardId, final Iterable<T> candidates, final Function<T, String> dirNameGetter) {
        T maximum = null;
        DynamicIndexShardInfo parsedMaximum = null;
        for (final T candidate : candidates) {
            if (candidate == null) {
                continue;
            }
            final String name = dirNameGetter.apply(candidate);
            if (name == null) {
                continue;
            }
            final Optional<DynamicIndexShardInfo> parsed = tryParse(name);
            if (parsed.isPresent() && parsed.get().getId().equals(shardId)) {
                if (maximum == null || parsedMaximum.compareTo(parsed.get()) < 0) {
                    maximum = candidate;
                    parsedMaximum = parsed.get();
                }
            }
        }
        return Optional.ofNullable(maximum);
    }
}
