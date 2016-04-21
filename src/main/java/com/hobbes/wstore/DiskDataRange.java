package com.hobbes.wstore;

class DiskDataRange extends DataRange {

    DiskLocation startLoc, endLoc;

    public DiskDataRange(long logicalStartPosition, long logicalEndPosition,
			 DiskLocation start, DiskLocation end) {
	super(logicalStartPosition, logicalEndPosition);
	startLoc = start;
	endLoc = end;
    }

    @Override
    public long getData(byte[] buf, int pos, int len) {
	// Do nothing for now
	return 0;
    }

    public DataRange getSubrange(long start, long end) {
	assert start >= getLogicalStartPosition();
	assert end <= getLogicalEndPosition();

	long startAbsOffset = startLoc.getAbsoluteOffset() + (start - getLogicalStartPosition());
	long endAbsOffset = startLoc.getAbsoluteOffset() + (end - getLogicalEndPosition());
	long blockSize = startLoc.getBlockSize();
	
	return new DiskDataRange(start, end, new DiskLocation(startAbsOffset, blockSize),
				 new DiskLocation(endAbsOffset, blockSize));
    }
    
}
