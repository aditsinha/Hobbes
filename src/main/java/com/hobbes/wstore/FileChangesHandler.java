package com.hobbes.wstore;

import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;
    
public class FileChangesHandler {

    private Path dataPath;
    private FileByteChanges byteChanges;
    private FileBlockChanges blockChanges;
	
    public static final long byteThreshold = 200000;
    

    public FileChangesHandler(FileSystem fileSystem, Path dataPath, Path blockLogPath, Path byteLogPath) throws IOException {
	this.dataPath = dataPath;
	byteChanges = new FileByteChanges(fileSystem, dataPath, byteLogPath);
	blockChanges = new FileBlockChanges(fileSystem, dataPath, blockLogPath);
    }

    public Path getDataPath() {
	return dataPath;
    }

    public void write(List<ByteArrayDataRange> changes) throws IOException {
	FileByteChangesDeque changeDeque = byteChanges.getDeque();
	for (ByteArrayDataRange change : changes) {
	    changeDeque.add(change);
	}
	if(changeDeque.getNumChanges() > byteThreshold) {
	    //	    System.out.println("FLUSHED");
	    byteFlush();
	}
    }

	public void byteFlush() throws IOException {
		FileByteChangesDeque fbcd = byteChanges.getDeque();
		blockChanges.incorporateChanges(fbcd);

		// I think you're allowed to do this, you might need to
		// clear the deque from the byteChanges Object
		fbcd.clearDeque();
	}

	/* Write everything to stable storage */
	public void sync() throws IOException {
		byteChanges.writeLog();
		blockChanges.sync();
	}
		
		
		
    public List<DataRange> read(long start, long len) {
	List<? extends DataRange> changeDeque = byteChanges.getDeque().getDeque();

	List<DataRange> ret  = new ArrayList<>();

	long currentByte = start;

	//	System.out.println("CHANGE DEQUE SIZE: " + byteChanges.getDeque().getDeque().size());

	for (int i = byteChanges.getDeque().search(start); i != -1 && i < changeDeque.size() && currentByte - start < len; i++) {

	    DataRange recentChangeRange = changeDeque.get(i);
	    long relevantStart = Math.max(start, recentChangeRange.getLogicalStartPosition());
	    long relevantEnd = Math.min(start + len, recentChangeRange.getLogicalEndPosition());

	    if (relevantStart > start + currentByte) {
		ret.addAll(blockChanges.resolve(start + currentByte, relevantStart));
		currentByte = relevantStart;
	    }

	    ret.add(recentChangeRange.getSubrange(relevantStart, relevantEnd));
	    currentByte = relevantEnd;
	}

	if (currentByte != start + len) {
	    ret.addAll(blockChanges.resolve(currentByte, start + len));
	}

	return ret;
	
    }

    public long getLastLogicalPosition() {
	return (long) Math.max(byteChanges.getLastLogicalPosition(), blockChanges.getLastLogicalPosition());
    }

    public void teardown() throws IOException {
	byteChanges.teardown();
	blockChanges.teardown();
    }
    /*
    public List<DataRange> readExperimental(long start, long len) {
	FileByteChangesDeque changeDeque = byteChanges.getDeque();

	List<DataRange> ret  = new ArrayList<>();

	List<DataRange> resolvedBlocks = blockChanges.resolve(start, start + len);

	int currentResolvedBlock = 0;
	long currentByte = 0;
	
	for (int i = byteChanges.search(start); i < changeDeque.size() && currentByte < len; i++) {
	    // check if this data range is pertinent
	    ByteArrayDataRange recentChangeRange = byteChanges.get(i);
	    long rangeStart = recentChangeRange.getLogicalStartPosition();
	    long rangeEnd = recentChangeRange.getLogicalEndPosition();
	    
	    if (rangeEnd <= start)
		continue;

	    if (rangeStart >= start + len)
		break;

	    // there is overlap!!!
	    
	    long relevantRangeStart = Math.max(start + currentByte, rangeStart);
	    long relevantRangeEnd = Math.min(start + len, rangeEnd);

	    while (relevantRangeStart > start + currentByte) {
		// we want to grab some stuff from the resolved blocks
		while (resolvedBlocks.get(currentResolvedBlock).getLogicalEndPosition() <= start + currentByte) {
		    currentResolvedBlock++;
		}

		// this should be the correct block
		assert resolvedBlocks.get(currentResolvedBlock).getLogicalStartPosition() <= start + currentByte;

		// consume as much as we can out of this block
		DataRange blockRange = resolvedBlocks.get(currentResolvedBlock);
		DataRange blockSubrange = blockRange.getSubrange(start + currentByte, Math.min(blockRange.getLogicalEndPosition(), rangeStart));

		ret.add(blockSubrange);

		currentByte += blockSubrange.size();
	    }

	    DataRange recentChangeSubrange = recentChangeRange.subrange(relevantRangeStart, relevantChangeEnd);

	    ret.add(recentChangeSubrange);
	    currentByte += recentChangeSubrange.size();
	}

	if (currentByte != len) {
	    // add all ranges that are left in the file block changes list

	    // find the first block that needs to be added and add a subrange of that
	    while (resolvedBlocks.get(currentResolvedBlock).getLogicalEndPosition() <= start + currentByte) {
		currentResolvedBlock++;
	    }

	    ret.add(resolvedBlocks.get(i).subrange(start + currentByte, resolvedBlocks.get(i).getLogicalEndPosition()));

	    for (int i = currentResolvedBlock; i < resolvedBlocks.size(); i++) {
		ret.add(resolvedBlocks.get(i));
	    }
	}

	return ret;
    }
    */
}


