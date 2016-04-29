package com.hobbes.wstore;

import java.io.*;
import org.apache.hadoop.fs.*;


class DiskDataRange extends DataRange {

    DiskLocation startLoc, endLoc;
	FSDataInputStream in;

    public DiskDataRange(FSDataInputStream in, long logicalStartPosition, long logicalEndPosition,
			 DiskLocation start, DiskLocation end) {
	super(logicalStartPosition, logicalEndPosition);
	this.in = in;
	startLoc = start;
	endLoc = end;
    }

    @Override
    public int getData(long relativeStartPosition, byte[] buf, int pos, int len) throws IOException {
	return in.read(startLoc.getAbsoluteOffset() + relativeStartPosition, buf, pos, len);
    }

    public DataRange getSubrange(long start, long end) {
	assert start >= getLogicalStartPosition();
	assert end <= getLogicalEndPosition();

	long startAbsOffset = startLoc.getAbsoluteOffset() + (start - getLogicalStartPosition());
	long endAbsOffset = startLoc.getAbsoluteOffset() + (end - getLogicalEndPosition());
	long blockSize = startLoc.getBlockSize();
	
	return new DiskDataRange(in, start, end, new DiskLocation(startAbsOffset, blockSize),
				 new DiskLocation(endAbsOffset, blockSize));
    }
}
