package com.hobbes.wstore;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;

public class FileChangesHandlerCoordinator {

	private static FileChangesHandlerCoordinator instance = null;
    private static final int CLOCK_CONSTANT = 3;
    private static final int SIZE = 5000;

    private static class FileCacheEntry {
		public FileChangesHandler handler;
		public int refCount;
		public int evictClock;
		public int clockArrayIndex;
		public Path path;

		public FileCacheEntry(FileChangesHandler handler, int clockArrayIndex, Path path) {
		    this.handler = handler;
		    this.clockArrayIndex = clockArrayIndex;
		    this.path = path;
		    refCount = 1;
		    evictClock = CLOCK_CONSTANT;
		}
    }

    private Map<Path, FileCacheEntry> table;
    private FileCacheEntry[] clockTable;
    private int clockIndex;
    
    private int size;
    private FileSystem fileSystem;
    private int currLeast;
    private int currIndex;

    protected FileChangesHandlerCoordinator() throws IOException {
		table = new HashMap<>();
		this.size = SIZE;
		this.fileSystem = FileSystemFactory.get();
		clockTable = new FileCacheEntry[2 * size];
		currLeast = currIndex = 0;
    }

    public static FileChangesHandlerCoordinator getInstance() throws IOException {
     	if(instance == null) {
        	instance = new FileChangesHandlerCoordinator();
     	}
    	return instance;
   }

    public synchronized FileChangesHandler get(Path dataPath, Path blockChangesLog, Path byteChangesLog) throws IOException {
	FileCacheEntry entry = table.get(dataPath);
	
	if (entry != null) {
	    entry.refCount++;
	    entry.evictClock = CLOCK_CONSTANT;
	    return entry.handler;
	}

	else {
	    if (table.size() == size) {
		evict();
	    }

	    FileChangesHandler handler = new FileChangesHandler(fileSystem, dataPath, blockChangesLog, byteChangesLog);
	    int i;
	    for (i = 0; i < 2*size; i++) {
		if (clockTable[i] == null) {
		    // found an empty spoty
		    break;
		}
	    }
		
	    entry = new FileCacheEntry(handler, i, dataPath);
	    clockTable[i] = entry;
	    table.put(dataPath, entry);
	}

	return entry.handler;
    }

    public synchronized void unget(FileChangesHandler handler) throws IOException {
	Path p = handler.getDataPath();
	table.get(p).refCount--;
	
	if (table.size() > size) {
	    evict();
	}
    }
    
    public void evict() throws IOException {
	int trialCount = 0;
	
	while (trialCount < (CLOCK_CONSTANT + 1)  * size) {
	    FileCacheEntry entry = clockTable[clockIndex];
	    
	    if (entry != null && entry.evictClock <= 0 && entry.refCount <= 0) {
		// evict me!
		entry.handler.sync();
		clockTable[clockIndex] = null;
		table.remove(entry.path);
	    } else if (entry != null && entry.refCount <= 0) {
		entry.evictClock--;
	    }

	    clockIndex = (clockIndex + 1) % clockTable.length;
	    trialCount++;
	}
    }

    public void tableSync() throws IOException {
	for(Map.Entry<Path, FileCacheEntry> cacheEntry : table.entrySet()) {
	    cacheEntry.getValue().handler.sync();
	    table.remove(cacheEntry.getKey());
	}
	
	for(int i=0; i < clockTable.length; i++) {
	    clockTable[i] = null;
	}

	clockIndex = 0;
    }
}
