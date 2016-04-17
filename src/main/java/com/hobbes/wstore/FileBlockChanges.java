package com.hobbes.wstore;

import java.util.*;

class FileBlockChanges {

    private Map<Long, Long> blockMap;

    private final long dataBlockSize;
    
    public FileByteChanges() {

	
    }

    public List<DataRange> resolve(long logicalStart, long logicalEnd) {
	List<DataRange> diskRanges = new ArrayList<>();

	long logicalBlock = logicalStart / dataBlockSize;
	long blockOffset = logicalStart % dataBlockSize;
	long physicalBlock = getPhysicalBlockNumber(logicalBlock);
	
	long currentBlockStartOffset = blockOffset;

	DiskLocation rangeStart = new DiskLocation(physicalBlock, blockOffset);


	int bytesRemaining = logicalEnd - logicalStart;

	while (bytesRemaining > 0) {
	    if (bytesRemaining <= dataBlockSize - currentBlockStartOffset) {
		// can finish with this block
		DiskLocation rangeEnd =
		    new DiskLocation(physicalBlock, currentBlockStartOffset + bytesRemaining);		
		diskRanges.add(new DiskRange(rangeStart, rangeEnd));
		bytesRemaining = 0;
	    } else {
		// we want to add this entire block to the current range.
		// check if the next block is sequentially located
		bytesRemaining -= dataBlockSize - currentBlockStartOffset;
		
		logicalBlock++;
		long nextPhysicalBlock = getPhysicalBlockNumber(logicalBlock);
		if (nextPhysicalBlock != physicalBlock + 1) {
		    // we can't include the next block in the same range, so terminate this
		    DiskLocation rangeEnd = new DiskLocation(physicalBlock, dataBlockSize);		
		    diskRanges.add(new DiskRange(rangeStart, rangeEnd));
		    rangeStart = new DiskRange(nextPhysicalBlock, 0);
		}

		physicalBlock = nextPhysicalBlock;
		currentBlockStartOffset = 0;
	    }
	}

	return diskRanges;
    }

    public long getPhysicalBlockNumber(long logicalBlock) {
	if (blockMap.contains(logicalBlock)) {
	    return blockMap.get(logicalBlock);
	}
	return logicalBlock;
    }
    
}

