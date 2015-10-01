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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
    Intended for course-grained instrumentation of Imhotep components. Note that
    this class is built for comfort, not for speed.
 */
public class Instrumentation {

    public interface Keys {
        public static final String EVENT_TYPE = "eventtype"; // reserved by Instrumentation.Event

        public static final String CPU_USER           = "cpuuser";
        public static final String CPU_TOTAL          = "cputotal";
        public static final String DATASET            = "dataset";
        public static final String ELAPSED_TM_NANOS   = "elapsedtmnanos";
        public static final String INT_METRICS        = "intmetrics";
        public static final String INT_METRIC_BYTES   = "intmetricbytes";
        public static final String MAX_USED_MEMORY    = "maxusedmemory";
        public static final String REQUEST_TYPE       = "requesttype";
        public static final String SESSION_ID         = "sessionid";
        public static final String SHARD_DATE         = "sharddate";
        public static final String SHARD_ID           = "shardid";
        public static final String SHARD_REQUEST_LIST = "shardrequestlist";
        public static final String SHARD_SIZE         = "shardsize";
        public static final String STATS_PUSHED       = "statspushed";
        public static final String STRING_FIELDS      = "stringfields";
        public static final String THREAD_FACTORY     = "threadfactory";
        public static final String THREAD_ID          = "threadid";
        public static final String USERNAME           = "username";
        public static final String USE_NATIVE_FTGS    = "usenativeftgs";
    }

    public static class Event {
        private final TreeMap<String, Object> properties = new TreeMap<String, Object>();

        public Event(final String type) { properties.put(Keys.EVENT_TYPE, type); }

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
        /** Note: You can add the same observer multiple times, but
            you will receive multiple events when they fire. */
        void addObserver(final Observer observer);

        /** Note: Attempting to remove a non-existent observer will fail
            silently.
            Note: Removing a multiply registered observer will only eliminate
            one copy. */
        void removeObserver(final Observer observer);
    }

    public static class ProviderSupport implements Provider {

        private final ArrayList<Observer> observers = new ArrayList<Observer>();

        public synchronized void    addObserver(Observer observer) { observers.add(observer);    }
        public synchronized void removeObserver(Observer observer) { observers.remove(observer); }

        public synchronized void fire(final Event event) {
            for (Observer observer: observers) {
                observer.onEvent(event);
            }
        }
    }
}
