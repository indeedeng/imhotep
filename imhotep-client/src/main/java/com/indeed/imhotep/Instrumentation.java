/*
 * Copyright (C) 2015 Indeed Inc.
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
package com.indeed.imhotep;

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
    Intended for course-grained instrumentation of Imhotep components. Note that
    this class is built for comfort, not for speed.
 */
public class Instrumentation {

    public interface Keys {
        public static final String EVENT_TYPE = "eventtype"; // reserved by Instrumentation.Event

        public static final String BEGIN_TIME_MILLIS   = "begintimemillis";
        public static final String CPU_TOTAL           = "cputotal";
        public static final String CPU_USER            = "cpuuser";
        public static final String DAEMON_THREAD_COUNT = "daemonthreadcount";
        public static final String DATASET             = "dataset";
        public static final String ELAPSED_TIME_MILLIS = "elapsedtimemillis";
        public static final String FIELDS              = "fields";
        public static final String FIELD_BYTES         = "fieldbytes";
        public static final String FREE_MEMORY         = "freememory";
        public static final String MAX_USED_MEMORY     = "maxusedmemory";
        public static final String METRICS             = "metrics";
        public static final String METRIC_BYTES        = "metricbytes";
        public static final String PEAK_THREAD_COUNT   = "peakthreadcount";
        public static final String REQUEST_SIZE        = "requestsize";
        public static final String REQUEST_TYPE        = "requesttype";
        public static final String RESPONSE_SIZE       = "responsesize";
        public static final String SEQ_NUM             = "seqnum";
        public static final String SESSION_ID          = "sessionid";
        public static final String SHARD_DATE          = "sharddate";
        public static final String SHARD_ID            = "shardid";
        public static final String SHARD_REQUEST_LIST  = "shardrequestlist";
        public static final String SHARD_SIZE          = "shardsize";
        public static final String SOURCE_ADDR         = "sourceaddr";
        public static final String STATS_PUSHED        = "statspushed";
        public static final String STATS_PUSHED_BYTES  = "statspushedbytes";
        public static final String STRING_FIELDS       = "stringfields";
        public static final String TARGET_ADDR         = "targetaddr";
        public static final String THREAD_COUNT        = "threadcount";
        public static final String THREAD_FACTORY      = "threadfactory";
        public static final String THREAD_ID           = "threadid";
        public static final String TOTAL_MEMORY        = "totalmemory";
        public static final String TOTAL_THREADS       = "totalthreads";
        public static final String USERNAME            = "username";
        public static final String USE_NATIVE_FTGS     = "usenativeftgs";
    }

    public static class Event {
        private static final AtomicLong seqNum = new AtomicLong();

        private final Object2ObjectArrayMap<String, Object> properties =
            new Object2ObjectArrayMap<String, Object>(16);

        public Event(final String type) {
            properties.put(Keys.EVENT_TYPE, type);
            properties.put(Keys.SEQ_NUM, seqNum.getAndIncrement());
        }

        public String getType() { return properties.get(Keys.EVENT_TYPE).toString(); }

        public Map<String, Object> getProperties() { return properties; }

        public String toString() { return new JSON().format(getProperties()); }

        private final static class JSON {

            static final String BEGIN_OBJ    = "{ ";
            static final String END_OBJ      = " }";
            static final String QUOTE        = "\"";
            static final String SEPARATOR    = ", ";
            static final String KV_SEPARATOR = " : ";

            String format(Object value) {
                StringBuilder result = new StringBuilder();
                result.append(QUOTE);
                result.append(value != null ? value.toString() : "(null)");
                result.append(QUOTE);
                return result.toString();
            }

            String format(Map.Entry<String, Object> entry) {
                StringBuilder result = new StringBuilder();
                result.append(format(entry.getKey()));
                result.append(KV_SEPARATOR);
                result.append(format(entry.getValue()));
                return result.toString();
            }

            String format(Map<String, Object> map) {
                StringBuilder result = new StringBuilder();
                result.append(BEGIN_OBJ);
                final Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
                while (it.hasNext()) {
                    result.append(format(it.next()));
                    if (it.hasNext()) result.append(SEPARATOR);
                }
                result.append(END_OBJ);
                return result.toString();
            }
        }
    }

    public interface Observer {
        void onEvent(final Event event);
    }

    public interface Provider {
        void addObserver(final Observer observer);
        void removeObserver(final Observer observer);
    }

    public static class ProviderSupport implements Provider {

        private final ObjectArraySet<Observer> observers = new ObjectArraySet<Observer>();

        public synchronized void    addObserver(Observer observer) { observers.add(observer);    }
        public synchronized void removeObserver(Observer observer) { observers.remove(observer); }

        public synchronized void fire(final Event event) {
            for (Observer observer: observers) {
                observer.onEvent(event);
            }
        }
    }
}
