package com.hobbes.wstore;

class DiskLocation {

    private final long blockNumber, blockOffset, blockSize;

    public DiskLocation(long blockNumber, long blockOffset, long blockSize) {
	this.blockNumber = blockNumber;
	this.blockOffset = blockOffset;
	this.blockSize = blockSize;
    }

    public DiskLocation(long absoluteOffset, long blockSize) {
	this(absoluteOffset / blockSize, absoluteOffset % blockSize, blockSize);
    }

    public long getBlockNumber() {
	return blockNumber;
    }

    public long getBlockOffset() {
	return blockOffset;
    }

    public long getBlockSize() {
	return blockSize;
    }

    public long getAbsoluteOffset() {
	return blockNumber * blockSize + blockOffset;
    }
}
