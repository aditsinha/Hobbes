package com.hobbes.wstore;

import java.io.*;
/**
 * Represents a range of data in a file.  The start is inclusive and
 * the end is exclusive
 */
abstract class DataRange {

    private long logicalStartPosition, logicalEndPosition;

    public DataRange(long logicalStartPosition, long logicalEndPosition) {
	this.logicalStartPosition = logicalStartPosition;
	this.logicalEndPosition = logicalEndPosition;
	
    }

    public long getLogicalStartPosition() {
	return logicalStartPosition;
    }

    public long getLogicalEndPosition() {
	return logicalEndPosition;
    }

	public void setLogicalStartPosition(long logicalStartPosition) {
		this.logicalStartPosition = logicalStartPosition;
	}

	public void setLogicalEndPosition(long logicalEndPosition) {
		this.logicalEndPosition = logicalEndPosition;
	}

    /**
     * Copy this data range into buf, starting at buf[pos]
     */
    public abstract int getData(long relativeStartPosition, byte[] buf, int pos, int len) throws IOException;

    public long size() {
	return logicalEndPosition - logicalStartPosition;
    }

    public abstract DataRange getSubrange(long logicalStart, long logicalEnd);
}
