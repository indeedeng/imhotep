/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.imhotep.client;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * @author jplaisance
 */
public final class ShardTimeUtils {
    private static final Logger log = Logger.getLogger(ShardTimeUtils.class);
    private static final DateTimeZone ZONE = DateTimeZone.forOffsetHours(-6);
    private static final DateTimeFormatter yyyymmdd = DateTimeFormat.forPattern("yyyyMMdd").withZone(ZONE);
    private static final DateTimeFormatter yyyymmddhh = DateTimeFormat.forPattern("yyyyMMdd.HH").withZone(ZONE);

    public static Interval parseInterval(String shardId) {
        if (shardId.length() > 16) {
            final DateTime start = yyyymmddhh.parseDateTime(shardId.substring(5, 16));
            final DateTime end = yyyymmddhh.parseDateTime(shardId.substring(17, 28));
            return new Interval(start, end);
        } else if (shardId.length() > 13) {
            final DateTime start = yyyymmddhh.parseDateTime(shardId.substring(5, 16));
            final DateTime end = start.plusHours(1);
            return new Interval(start, end);
        } else {
            final DateTime start = yyyymmdd.parseDateTime(shardId.substring(5, 13));
            final DateTime end = start.plusDays(1);
            return new Interval(start, end);
        }
    }
}
