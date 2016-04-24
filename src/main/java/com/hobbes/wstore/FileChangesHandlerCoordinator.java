package com.hobbes.wstore;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;

public class FileChangesHandlerCoordinator {
    private Map<Path, FileChangesHandler> table;
    private Map<Path, Integer> refCount;
    private int size;
    private FileSystem fileSystem;
    private Path[] lru;
    private int currLeast;
    private int currIndex;

    public FileByteChangesCoordinator(FileSystem fileSystem, int size) {
	table = new HashMap<Path, FileChangesHandler>();
	this.size = size;
	this.fileSystem = fileSystem;
	lru = new Path[size];
	currLeast = currIndex = 0;
    }

    public synchronized FileChangesHandler get(Path dataPath, Path blockChangesLog, Path byteChangesLog) {
	if (table.containsKey(daatPath)) {
	    refCount.put(dataPath, refCount.get(dataPath) + 1);
	    return table.get(dataPath);
	}

	else {
	    if (table.size() == size) {
		evict();
	    }

	    FileChangesHandler handler = new FileChangesHandler(fileSystem, dataPath, blockChangesLog, byteChangesLog);
	    
	    table.put(dataPath, handler);
	    refCount.put(dataPath, 1);
	    
	}
    }

    public synchronized void unget(FileChangesHandler handler) {
	Path p = handler.getDataPath();
	refCount.put(p, refCount.get(p) - 1);

	if (table.size() > size) {
	    evict();
	}
    }
    
    public FileByteChanges evict() {
	Path least = lru[currLeast];
	lru[currLeast] = null;
	FileByteChanges ret = table.remove(least);
	ret.writeLog();
	currLeast = (currLeast + 1) % size;
	return ret;
    }

    public void tableFlush() {
	for(Map.Entry<Path, FileByteChanges> entry : table.entrySet()) {
	    entry.getValue().writeLog();
	    table.remove(entry.getKey());
	}
	
	for(int i=0; i < size; i++) {
	    lru[i] = null;
	}
	currLeast = currIndex = 0;
    }

		
}
