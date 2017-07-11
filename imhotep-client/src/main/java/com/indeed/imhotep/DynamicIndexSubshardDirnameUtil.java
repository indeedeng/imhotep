package com.indeed.imhotep;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author michihiko
 */
public final class DynamicIndexSubshardDirnameUtil {
    private static final Pattern DYNAMIC_INDEX_ID_PATTERN = Pattern.compile("^(dindex.+\\.(\\d{1,9})\\.(\\d{1,9}))$");
    private static final Pattern DYNAMIC_INDEX_NAME_PATTERN = Pattern.compile("^(dindex.+\\.(\\d{1,9})\\.(\\d{1,9}))\\.(\\d{1,18})\\.(\\d{1,18})$");
    private static final DateTimeZone ZONE = DateTimeZone.forOffsetHours(-6);
    private static final DateTimeFormatter yyyymmddhh = DateTimeFormat.forPattern("yyyyMMdd.HH").withZone(ZONE);

    private DynamicIndexSubshardDirnameUtil() {
    }

    public static String getShardId(final long startTimeMillis, final long endTimeMillis, final int subshardId, final int numSubshards) {
        return "dindex"
                + yyyymmddhh.print(startTimeMillis)
                + '-'
                + yyyymmddhh.print(endTimeMillis)
                + '.'
                + subshardId
                + '.'
                + numSubshards;
    }

    public static class ParseResult implements Comparable<ParseResult> {
        private final String name;
        private final String id;
        private final long updateId;
        private final int subshardId;
        private final int numSubshards;
        private final long timestamp;

        private ParseResult(@Nonnull final String name) throws IllegalArgumentException {
            this.name = name;
            final Matcher dynamicMatcher = DYNAMIC_INDEX_NAME_PATTERN.matcher(name);
            if (dynamicMatcher.matches()) {
                this.id = dynamicMatcher.group(1);
                this.updateId = Long.parseLong(dynamicMatcher.group(4));
                this.timestamp = Long.parseLong(dynamicMatcher.group(5));
                this.subshardId = Integer.parseInt(dynamicMatcher.group(2));
                this.numSubshards = Integer.parseInt(dynamicMatcher.group(3));
            } else {
                throw new IllegalArgumentException(this.name + " is invalid name for dynamic index");
            }
        }

        @Override
        public int compareTo(@Nonnull final ParseResult o) {
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
            if (!(o instanceof ParseResult)) {
                return false;
            }
            final ParseResult that = (ParseResult) o;
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

    public static ParseResult parse(final String name) {
        return new ParseResult(name);
    }

    public static Optional<ParseResult> tryParse(final String name) {
        try {
            return Optional.of(parse(name));
        } catch (final IllegalArgumentException e) {
            return Optional.absent();
        }
    }

    public static boolean isValidName(final String name) {
        return DYNAMIC_INDEX_NAME_PATTERN.matcher(name).matches();
    }

    public static boolean isValidShardId(final String shardId) {
        return DYNAMIC_INDEX_ID_PATTERN.matcher(shardId).matches();
    }

    public static int parseSubshardIdFromShardId(final String shardId) {
        final Matcher matcher = DYNAMIC_INDEX_ID_PATTERN.matcher(shardId);
        Preconditions.checkArgument(matcher.matches());
        return Integer.parseInt(matcher.group(2));
    }

    /**
     * For iterable of T, where each element corresponds to a subshard, select the element that corresponding to the latest subshard.
     *
     * @param shardId    Id of the subshard to search, e.g. dindex20170705.02-20170705.03.1.4
     * @param candidates Iterable of T, e.g. List&lt;Path&gt;. nulls are ignored.
     * @param nameGetter Converter from T to the name of the subshard, e.g. (path) -> path.getFileName().toString(). Ignored if the return value is null.
     */
    public static <T> Optional<T> selectLatest(final String shardId, final Iterable<T> candidates, final Function<T, String> nameGetter) {
        T maximum = null;
        ParseResult parsedMaximum = null;
        for (final T candidate : candidates) {
            if (candidate == null) {
                continue;
            }
            final String name = nameGetter.apply(candidate);
            if (name == null) {
                continue;
            }
            final Optional<ParseResult> parsed = tryParse(name);
            if (parsed.isPresent() && parsed.get().getId().equals(shardId)) {
                if (maximum == null || parsedMaximum.compareTo(parsed.get()) < 0) {
                    maximum = candidate;
                    parsedMaximum = parsed.get();
                }
            }
        }
        return Optional.fromNullable(maximum);
    }
}
