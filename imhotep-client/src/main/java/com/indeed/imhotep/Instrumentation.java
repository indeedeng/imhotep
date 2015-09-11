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

import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class Instrumentation {

    static public class Event {
        private final String              type;
        private final Map<Object, Object> properties = new HashMap<Object, Object>();

        // !@# One shouldn't have to pass in an external property map - just create one internally
        public Event(final String type) {
            this.type = type;
        }

        public String                    getType() { return type;       }
        public Map<Object, Object> getProperties() { return properties; }

        public String toString() {
            StringBuilder result = new StringBuilder();
            result.append('[');
            result.append(getType());
            result.append(' ');
            result.append(toString(getProperties()));
            result.append(']');
            return result.toString();
        }

        private String toString(Object value) {
            if (value instanceof Map) {
                final StringBuilder result = new StringBuilder();
                final Iterator<Map.Entry> it = ((Map) value).entrySet().iterator();
                result.append("[");
                while (it.hasNext()) {
                    final Map.Entry entry = it.next();
                    result.append(entry.getKey());
                    result.append(':');
                    result.append(toString(entry.getValue()));
                    if (it.hasNext()) result.append(' ');
                }
                result.append("]");
                return result.toString();
            }
            return value != null ? value.toString() : "null";
        }
    }

    public interface Observer {
        void onEvent(final Event event);
    }

    public interface Provider {
        /** Note: You can add the same observer for the same event multiple
            times, but you will receive multiple events when they fire. */
        void addObserver(final String event, final Observer observer);

        /** Note: Attempting to remove a non-existent observer will fail
            silently.
            Note: Removing a multiply registered observer will only eliminate
            one copy. */
        void removeObserver(final String event, final Observer observer);
    }

    public static class ProviderSupport implements Provider {

        private final HashMap<String, LinkedList<Observer>> observers
            = new HashMap<String, LinkedList<Observer>>();

        public void addObserver(final String event, final Observer observer) {
            LinkedList<Observer> observerList = observers.get(event);
            if (observerList == null) {
                observerList = new LinkedList<Observer>();
                observers.put(event, observerList);
            }
            observerList.add(observer);
        }

        public void removeObserver(final String event, final Observer observer) {
            LinkedList<Observer> observerList = observers.get(event);
            if (observerList != null) {
                observerList.remove(observer);
            }
        }

        public void fire(final Event event) {
            LinkedList<Observer> observerList = observers.get(event.getType());
            if (observerList != null) {
                for (Observer observer: observerList) {
                    observer.onEvent(event);
                }
            }
        }
    }
}
