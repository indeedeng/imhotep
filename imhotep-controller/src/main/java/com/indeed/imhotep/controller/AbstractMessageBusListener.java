package com.indeed.imhotep.controller;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by darren on 3/8/16.
 */
public abstract class AbstractMessageBusListener<T> {
    protected final String topicPrefix;
    protected final Pattern pattern;
    protected final boolean requireExclusiveMatch;
    protected final DataBinder<T> dataBinder;

    public AbstractMessageBusListener(String topicPrefix,
                                      Pattern pattern,
                                      boolean requireExclusiveMatch,
                                      DataBinder<T> dataBinder) {
        this.topicPrefix = topicPrefix;
        this.pattern = pattern;
        this.requireExclusiveMatch = requireExclusiveMatch;
        this.dataBinder = dataBinder;
    }

    public void execute(String topic, byte[] dataBuffer) throws Exception {
        final T data = this.dataBinder.bind(dataBuffer);
        processMessage(topic, data);
    }

    // test for match?
    public boolean matches(String topic) {
        final Matcher matcher = this.pattern.matcher(topic);
        return matcher.matches();
    }

    // process message
    public abstract void processMessage(String topic, T data) throws Exception;

    public String getTopicPrefix() {
        return topicPrefix;
    }

    public Pattern getPattern() {
        return pattern;
    }

    public boolean isRequireExclusiveMatch() {
        return requireExclusiveMatch;
    }

    public DataBinder<T> getDataBinder() {
        return dataBinder;
    }

    public static abstract class DataBinder<T> {
        public abstract T bind(byte[] data);
    }
}
