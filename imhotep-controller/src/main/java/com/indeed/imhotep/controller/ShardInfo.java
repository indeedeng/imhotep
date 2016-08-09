package com.indeed.imhotep.controller;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by darren on 2/26/16.
 */
public class ShardInfo {
    private static final DateTimeFormatter yyyymmdd =
            DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHours(-6));
    private static final DateTimeFormatter yyyymmddhh =
            DateTimeFormat.forPattern("yyyyMMdd.HH").withZone(DateTimeZone.forOffsetHours(-6));

    private static final Pattern VERSION_PATTERN = Pattern.compile("^(.+)\\.(\\d{14})$");

    public static final int FORMAT_FLAMDEX = 0;
    public static final int FORMAT_LUCENE = 1;


    public int id;
    public int datasetId;
    public long startTime;
    public long endTime;
    public long version;
    public byte format;
    public String filename;

    ShardInfo() {
        resetToNull();
    }

    public ShardInfo(int id,
                     int dataset_id,
                     long start_time,
                     long end_time,
                     long version,
                     byte format) {
        this.id = id;
        this.datasetId = dataset_id;
        this.startTime = start_time;
        this.endTime = end_time;
        this.version = version;
        this.format = format;
    }

    void reset(String shardFileName) throws ShardNameParseException {
        final String shardName;

        resetToNull();
        this.filename = shardFileName;

        /* determine shard type */
        if (shardFileName.endsWith(".gz") || shardFileName.endsWith(".tar")) {
            this.format = FORMAT_LUCENE;
            shardName = shardFileName.replace(".tar", "").replace(".gz", "");
        } else if (shardFileName.endsWith(".sqar")) {
            this.format = FORMAT_FLAMDEX;
            shardName = shardFileName.replace(".sqar", "");
        } else {
            throw new ShardNameParseException("Unknown shard format for shard "
                                                      + "'" + shardFileName + "'");
        }

        try {
            switch (format) {
                case FORMAT_FLAMDEX:
                    final String id;
                    final Matcher matcher = VERSION_PATTERN.matcher(shardName);
                    if (matcher.matches()) {
                        id = matcher.group(1);
                        this.version = Long.parseLong(matcher.group(2));
                    } else {
                        id = shardName;
                        this.version = 0L;
                    }

                    if (id.length() > 13) {
                        startTime = yyyymmddhh.parseMillis(id.substring(5, 16));
                        endTime = startTime + (24 * 60 * 60 * 1000); // startTime + 1 day
                    } else {
                        startTime = yyyymmdd.parseMillis(id.substring(5, 13));
                        endTime = startTime + (24 * 60 * 60 * 1000); // startTime + 1 day
                    }
                    break;
                case FORMAT_LUCENE:
                    startTime = yyyymmddhh.parseMillis(shardName.substring(5, 16));
                    endTime = yyyymmddhh.parseMillis(shardName.substring(17, 28));
                    this.version = 0;
            }
        } catch (IllegalArgumentException e) {
            throw new ShardNameParseException("Unknown shard format for shard " + shardName, e);
        }
    }

    private void resetToNull() {
        this.id = -1;
        this.datasetId = -1;
        this.startTime = 0;
        this.endTime = 0;
        this.version = 0;
        this.format = -1;
        this.filename = null;
    }

    public class ShardNameParseException extends Exception {
        public ShardNameParseException(String message) {
            super(message);
        }

        public ShardNameParseException(String message, IllegalArgumentException e) {
            super(message, e);
        }
    }
}
