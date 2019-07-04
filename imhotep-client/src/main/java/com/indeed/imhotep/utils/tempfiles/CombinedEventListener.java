package com.indeed.imhotep.utils.tempfiles;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class CombinedEventListener implements EventListener {
    private final List<EventListener> eventListeners;

    public CombinedEventListener(final List<EventListener> eventListeners) {
        this.eventListeners = eventListeners;
    }

    public static CombinedEventListener of(final EventListener... eventListeners) {
        return new CombinedEventListener(ImmutableList.copyOf(eventListeners));
    }

    @Override
    public void removeTwice(final TempFileState tempFileState) {
        eventListeners.forEach(listener -> listener.removeTwice(tempFileState));
    }

    @Override
    public void removeReferencedFile(final TempFileState tempFileState) {
        eventListeners.forEach(listener -> listener.removeReferencedFile(tempFileState));
    }

    @Override
    public void didNotCloseInputStream(final TempFileState tempFileState) {
        eventListeners.forEach(listener -> listener.didNotCloseInputStream(tempFileState));
    }

    @Override
    public void didNotCloseOutputStream(final TempFileState tempFileState) {
        eventListeners.forEach(listener -> listener.didNotCloseOutputStream(tempFileState));
    }

    @Override
    public void didNotRemoveTempFile(final TempFileState tempFileState) {
        eventListeners.forEach(listener -> listener.didNotRemoveTempFile(tempFileState));
    }

    @Override
    public void expired(final TempFileState tempFileState) {
        eventListeners.forEach(listener -> listener.expired(tempFileState));
    }
}
