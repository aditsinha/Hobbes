package com.hobbes.wstore;  


import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;

class FileBlockChanges {

    private Map<Long, Long> blockMap;
    
    private final long dataBlockSize;

    private long nextPhysicalBlock;

    private FSDataInputStream dataIn;
    private FSDataOutputStream dataOut;
    private FSDataOutputStream logOut;

    private long logicalFileSize;
    
    public FileBlockChanges(FileSystem fileSystem, Path dataFile, Path logFile) throws IOException {
	FileStatus status = fileSystem.getFileStatus(dataFile);

	this.dataBlockSize = status.getBlockSize();
	
	long fileSize = status.getLen();
	assert fileSize % dataBlockSize == 0;
	this.nextPhysicalBlock = fileSize / dataBlockSize;

	this.dataIn = fileSystem.open(dataFile);
	this.dataOut = fileSystem.append(dataFile);
	this.logOut = fileSystem.append(logFile);

	FSDataInputStream logIn = fileSystem.open(logFile);

	readLogFile(logIn);
    }

    private void readLogFile(DataInputStream logIn) throws IOException {
	// we want to read the log in order to create the block map.
	while (logIn.available() > 0) {
	    char which = logIn.readChar();
	    if (which == 's') {
		long size = logIn.readLong();
		logicalFileSize = size;
	    } else if (which == 'c') {
		long logical = logIn.readLong();
		long physical = logIn.readLong();
		blockMap.put(logical, physical);
	    }
	}
    }
    
    public List<DataRange> resolve(long logicalStart, long logicalEnd) {
	List<DataRange> diskRanges = new ArrayList<>();

	long logicalBlock = logicalStart / dataBlockSize;
	long blockOffset = logicalStart % dataBlockSize;
	long physicalBlock = getPhysicalBlockNumber(logicalBlock);
	
	long currentBlockStartOffset = blockOffset;

	DiskLocation rangeStart = new DiskLocation(physicalBlock, blockOffset, dataBlockSize);

	long bytesRemaining = logicalEnd - logicalStart;
	long rangeStartLogical = logicalStart;
	
	while (bytesRemaining > 0) {
	    if (bytesRemaining <= dataBlockSize - currentBlockStartOffset) {
		// can finish with this block
		DiskLocation rangeEnd =
		    new DiskLocation(physicalBlock, currentBlockStartOffset + bytesRemaining, dataBlockSize);
		diskRanges.add(new DiskDataRange(dataIn, rangeStartLogical, logicalEnd,
						 rangeStart, rangeEnd));
		bytesRemaining = 0;
	    } else { 
		// we want to add this entire block to the current range.
		// check if the next block is sequentially located
		bytesRemaining -= dataBlockSize - currentBlockStartOffset;
		
		logicalBlock++;
		long nextPhysicalBlock = getPhysicalBlockNumber(logicalBlock);
		if (nextPhysicalBlock != physicalBlock + 1) {
		    // we can't include the next block in the same range, so terminate this
		    DiskLocation rangeEnd = new DiskLocation(physicalBlock, dataBlockSize, dataBlockSize);
		    long rangeEndLogical = (logicalEnd - logicalStart) - bytesRemaining;
		    diskRanges.add(new DiskDataRange(dataIn, rangeStartLogical, rangeEndLogical,
						     rangeStart, rangeEnd));
		    rangeStart = new DiskLocation(nextPhysicalBlock, 0, dataBlockSize);
		    rangeStartLogical = rangeEndLogical;
		}
		
		physicalBlock = nextPhysicalBlock;
		currentBlockStartOffset = 0;
	    }
	}

	return diskRanges;
    }

    public long getPhysicalBlockNumber(long logicalBlock)  {
	if (blockMap.containsKey(logicalBlock)) {
	    return blockMap.get(logicalBlock);
	}
	return logicalBlock;
    }
	
    

    public void incorporateChanges(FileByteChangesDeque fbcd) throws IOException {
	byte[] rewriteBlock = new byte[(int)dataBlockSize];
	
	List<ByteArrayDataRange> changes = fbcd.getDeque();
    	if (changes.isEmpty()) {
	    return;
	}

	long lastChangeLogicalBlock = -1;
	int lastChangeEndOffset = 0;

	long newFileSize = logicalFileSize;
	
	int i = 0;
	ByteArrayDataRange change = null;

	while (change != null || i < changes.size()) {
	    if (change == null) {
		change = changes.get(i);
		i++;
	    }
	    
	    change = changes.get(i);
	    
	    long changeStartLogicalBlock = change.getLogicalStartPosition() / dataBlockSize;
	    int changeStartByteOffset = (int) (change.getLogicalStartPosition() % dataBlockSize);
	    int changeEndByteOffset = (int) (change.getLogicalEndPosition() % dataBlockSize);

	    if (lastChangeLogicalBlock == changeStartLogicalBlock) {
		// we want to copy from the end offset of the last
		// change until the start of my change from disk
		assert changeStartByteOffset >= lastChangeEndOffset;
		dataIn.read(dataBlockSize * changeStartLogicalBlock + lastChangeEndOffset,
			    rewriteBlock, lastChangeEndOffset, changeStartByteOffset - lastChangeEndOffset);
	    } else {
		// copy from the beginning of the block to the start
		// of my change from disk
		dataIn.read(dataBlockSize * changeStartLogicalBlock,
			    rewriteBlock, 0, changeStartByteOffset);
	    }

	    // copy my change into the rewrite block 
	    int bytesToCopy = (int) Math.min(change.size(), dataBlockSize - changeStartByteOffset);


	    change.getData(0, rewriteBlock, (int)changeStartByteOffset, bytesToCopy);
	    lastChangeLogicalBlock = changeStartLogicalBlock;

	    if (changeStartLogicalBlock == newFileSize / dataBlockSize) {
		// working in the last block.  need to increment the file size by the amount I added
		int oldFileEndOffset = (int) (newFileSize % dataBlockSize);
		int newFileEndOffset = (int) changeStartByteOffset + bytesToCopy;
		newFileSize += (newFileEndOffset - oldFileEndOffset);
	    }

	    boolean shouldFlushBlock = true;
	    if (bytesToCopy < change.size()) {
		// we want to slice the current change and create a
		// new change from it that starts at the next block
		change = new ByteArrayDataRange(change.getLogicalStartPosition() + bytesToCopy,
						change.getLogicalEndPosition(),
						Arrays.copyOfRange(change.backing,
								   bytesToCopy, (int)change.size()));
		shouldFlushBlock = true;
	    } else {
		// check if the next change in the array is in the same block as this
		boolean nextInSameBlock = false;
		
		if (i < changes.size() - 1) {
		    ByteArrayDataRange nextChange = changes.get(i+1);
		    if (nextChange.getLogicalStartPosition() / dataBlockSize == changeStartLogicalBlock) {
			nextInSameBlock = true;
			shouldFlushBlock = false;
		    }
		}

		if (!nextInSameBlock) {
		    shouldFlushBlock = true;
		    // get data from the end of the change to the end of the block
		    dataIn.read(dataBlockSize * changeStartLogicalBlock + changeEndByteOffset,
				rewriteBlock, changeEndByteOffset, (int) (dataBlockSize - lastChangeEndOffset));
		}
		change = null;
	    }

	    if (shouldFlushBlock) {
		dataOut.write(rewriteBlock, 0, (int)dataBlockSize);
		logOut.writeChar('b');
		logOut.writeLong(changeStartLogicalBlock);
		logOut.writeLong(nextPhysicalBlock);
		blockMap.put(changeStartLogicalBlock, nextPhysicalBlock);

		nextPhysicalBlock++;
	    }

	    lastChangeLogicalBlock = changeStartLogicalBlock;
	    lastChangeEndOffset = changeEndByteOffset;
	}

	if (newFileSize != logicalFileSize) {
	    logOut.writeChar('s');
	    logOut.writeLong(newFileSize);
	    logicalFileSize = newFileSize;
	}

	logOut.hsync();
	dataOut.hsync();
    }

    public long getLastLogicalPosition() {
	return logicalFileSize;
    }

    /* Write to stable storage */
    public void sync() {

    }
}
