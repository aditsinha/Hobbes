package com.hobbes.wstore;

class DiskDataRange extends DataRange {

    public DiskDataRange(long logicalStartPosition, long logicalEndPosition,
			 DiskLocation start, DiskLocation end) {
	super(logicalStartPosition, logicalEndPosition);
    }

    @Override
    public long getData(byte[] buf, int pos, int len) {
	// Do nothing for now
	return 0;
    }
    
    
}
