package com.hobbes.wstore;

class DiskDataRange extends DataRange {

    public DiskDataRange(long logicalStartPosition, long logicalEndPosition,
			 DiskLocation start, DiskLocation end) {
	super(logicalStartPosition, logicalEndPosition);
    }

    @Override
    public int getData(byte[] buf, int pos) {
	// Do nothing for now
	return 0;
    }
    
    
}
