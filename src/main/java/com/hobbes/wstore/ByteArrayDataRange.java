package com.hobbes.wstore;

import java.util.Arrays;

class ByteArrayDataRange extends DataRange {

    public byte[] backing;
    
    public ByteArrayDataRange(long logicalStartPosition, long logicalEndPosition, byte[] backing) {
	super(logicalStartPosition, logicalEndPosition);
	assert backing.length == logicalEndPosition - logicalStartPosition;
	this.backing = backing;
    }
    
    @Override
    public long getData(long relativeStartPosition, byte[] buf, int pos, int len) {
	int toCopy = (int) Math.min(len, getLogicalEndPosition() - getLogicalStartPosition());
	System.arraycopy(backing, 0, buf, pos, toCopy);
	return toCopy;
    }

	public void setBacking(byte[] backing) {
		assert backing.length == this.getLogicalEndPosition() - this.getLogicalStartPosition();
		this.backing = backing;
	}

    public DataRange getSubrange(long start, long end) {
	assert start >= getLogicalStartPosition();
	assert end <= getLogicalEndPosition();

	if (start == getLogicalStartPosition() && end == getLogicalEndPosition()) {
	    return this;
	}
	
	return new ByteArrayDataRange(start, end,Arrays.copyOfRange(backing,
								    (int)(start - getLogicalStartPosition()),
								    (int)(end - getLogicalStartPosition())));
	
    }
	
}


