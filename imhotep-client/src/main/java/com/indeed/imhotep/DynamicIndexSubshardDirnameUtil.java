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
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author michihiko
 */
public final class DynamicIndexSubshardDirnameUtil {
    private static final Pattern DYNAMIC_INDEX_ID_PATTERN = Pattern.compile("^(dindex(\\d{8}\\.\\d{2})-(\\d{8}\\.\\d{2}).(\\d{1,9})\\.(\\d{1,9}))$");
    private static final Pattern DYNAMIC_INDEX_NAME_PATTERN = Pattern.compile("^(dindex(?:\\d{8}\\.\\d{2})-(?:\\d{8}\\.\\d{2})\\.(\\d{1,9})\\.(\\d{1,9}))\\.(\\d{1,18})\\.(\\d{1,18})$");
    private static final DateTimeZone ZONE = DateTimeZone.forOffsetHours(-6);
    private static final DateTimeFormatter YYYYMMDDHH = DateTimeFormat.forPattern("yyyyMMdd.HH").withZone(ZONE);

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
        private final long timestamp;
        private final int subshardId;
        private final int numSubshards;
        private final long version;

        private DynamicIndexShardInfo(@Nonnull final String name) throws IllegalArgumentException {
            this.name = name;
            final Matcher dynamicMatcher = DYNAMIC_INDEX_NAME_PATTERN.matcher(name);
            if (dynamicMatcher.matches()) {
                this.id = dynamicMatcher.group(1);
                this.timestamp = Long.parseLong(dynamicMatcher.group(4));
                this.version = Long.parseLong(dynamicMatcher.group(5));
                this.subshardId = Integer.parseInt(dynamicMatcher.group(2));
                this.numSubshards = Integer.parseInt(dynamicMatcher.group(3));
            } else {
                throw new IllegalArgumentException(this.name + " is invalid name for dynamic index");
            }
        }

        @Override
        public int compareTo(@Nonnull final DynamicIndexShardInfo o) {
            return ComparisonChain.start()
                    .compare(id, o.id)
                    .compare(timestamp, o.timestamp)
                    .compare(version, o.version)
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

        public long getTimestamp() {
            return timestamp;
        }

        public long getVersion() {
            return version;
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

    public static boolean isValidName(final String name) {
        return DYNAMIC_INDEX_NAME_PATTERN.matcher(name).matches();
    }

    public static boolean isValidShardId(final String shardId) {
        return DYNAMIC_INDEX_ID_PATTERN.matcher(shardId).matches();
    }

    public static DateTime parseStartTimeFromShardId(final String shardId) {
        final Matcher matcher = DYNAMIC_INDEX_ID_PATTERN.matcher(shardId);
        Preconditions.checkArgument(matcher.matches());
        return YYYYMMDDHH.parseDateTime(matcher.group(2));
    }

    public static Interval parseTimeRangeFromShardId(final String shardId) {
        final Matcher matcher = DYNAMIC_INDEX_ID_PATTERN.matcher(shardId);
        Preconditions.checkArgument(matcher.matches());
        final DateTime start = YYYYMMDDHH.parseDateTime(matcher.group(2));
        final DateTime end = YYYYMMDDHH.parseDateTime(matcher.group(3));
        return new Interval(start, end);
    }

    public static int parseSubshardIdFromShardId(final String shardId) {
        final Matcher matcher = DYNAMIC_INDEX_ID_PATTERN.matcher(shardId);
        Preconditions.checkArgument(matcher.matches());
        return Integer.parseInt(matcher.group(4));
    }

    /**
     * For iterable of T, where each element corresponds to a subshard, select the element that corresponding to the latest subshard.
     *
     * @param shardId       Id of the subshard to search, e.g. dindex20170705.02-20170705.03.1.4
     * @param candidates    Iterable of T, e.g. List&lt;Path&gt;. nulls are ignored.
     * @param dirNameGetter Converter from T to the name of the subshard, e.g. (path) -> path.getFileName().toString(). Ignored if the return value is null.
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
