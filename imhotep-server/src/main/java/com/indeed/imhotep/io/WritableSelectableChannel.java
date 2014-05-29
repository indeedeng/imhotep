package com.indeed.imhotep.io;

import java.nio.channels.SelectableChannel;
import java.nio.channels.WritableByteChannel;

/**
 * @author jplaisance
 */
public abstract class WritableSelectableChannel extends SelectableChannel implements WritableByteChannel {}
