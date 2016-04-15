package com.hobbes.wstore;

class DiskLocation {

    private final long blockNumber, blockOffset;

    public DiskLocation(long blockNumber, long blockOffset) {
	this.blockNumber = blockNumber;
	this.blockOffset = blockOffset;
    }

    public long getBlockNumber() {
	return blockNumber;
    }

    public long getBlockOffset() {
	return blockOffset;
    }
}
