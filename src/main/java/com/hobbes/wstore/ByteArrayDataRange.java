package com.hobbes.wstore;

class ByteArrayDataRange extends DataRange {

    byte[] backing;
    
    public ByteArrayDataRange(long logicalStartPosition, long logicalEndPosition, byte[] backing) {
	super(logicalStartPosition, logicalEndPosition);
	assert backing.length == logicalEndPosition - logicalStartPosition;
	this.backing = backing;
    }
    
    @Override
    public int getData(byte[] buf, int pos) {
	System.arraycopy(backing, 0, buf, pos, backing.length);
	return this.getLogicalEndPosition() - this.getLogicalStartPosition();
    }

}


