package com.indeed.imhotep.io;

import java.nio.channels.GatheringByteChannel;

/**
 * @author jplaisance
 */
public abstract class GatheringSelectableChannel extends WritableSelectableChannel implements GatheringByteChannel {}
