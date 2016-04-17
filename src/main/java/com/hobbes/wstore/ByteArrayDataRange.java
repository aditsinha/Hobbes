package com.hobbes.wstore;

class ByteArrayDataRange extends DataRange {

    public byte[] backing;
    
    public ByteArrayDataRange(long logicalStartPosition, long logicalEndPosition, byte[] backing) {
	super(logicalStartPosition, logicalEndPosition);
	assert backing.length == logicalEndPosition - logicalStartPosition;
	this.backing = backing;
    }
    
    @Override
    public long getData(byte[] buf, int pos, int len) {
	int toCopy = (int) Math.min(len, getLogicalEndPosition() - getLogicalStartPosition());
	System.arraycopy(backing, 0, buf, pos, toCopy);
	return toCopy;
    }

	public void setBacking(byte[] backing) {
		assert backing.length == this.getLogicalEndPosition() - this.getLogicalStartPosition();
		this.backing = backing;
	}

	
}


