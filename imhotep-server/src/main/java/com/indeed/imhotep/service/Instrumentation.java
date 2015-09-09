package com.indeed.imhotep.service;

import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class Instrumentation {

    static public class Event {
        private final String              type;
        private final Map<Object, Object> properties;

        public Event(final String type, final Map<Object, Object> properties) {
            this.type       = type;
            this.properties = properties;
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

    static class ProviderSupport implements Provider {

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
