package com.hobbes.wstore;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;

public class FileByteChangesCoordinator {
  	private Map<Path, FileByteChanges> table;
	private int size;
	private FileSystem fileSystem;
	private Path[] lru;
	private int currLeast;
	private int currIndex;

	public FileByteChangesCoordinator(FileSystem fileSystem, int size) {
		table = new HashMap<Path, FileByteChanges>();
		this.size = size;
		this.fileSystem = fileSystem;
		lru = new Path[size];
		currLeast = currIndex = 0;
	}

	public FileByteChanges insert(Path dataFile, Path logFile) throws IOException {
		FileByteChanges fbc = new FileByteChanges(fileSystem, dataFile, logFile);
		if(table.size() == size) {
			FileByteChanges ev = evict();
			ev.writeLog();
		}
		table.put(dataFile, fbc);
		lru[currIndex] = dataFile;
		currIndex = (currIndex + 1) % size;
		return fbc;
	}

	public boolean check(Path dataFile) {
		return (table.containsKey(dataFile));
	}
	
	public FileByteChangesDeque read(Path dataFile, Path logFile) throws IOException {
		if(!check(dataFile)) {
			insert(dataFile, logFile);
		}
		FileByteChangesDeque ret = table.get(dataFile).getDeque();
		return ret;
	}
	
	public void write(Path dataFile, Path logFile, ByteArrayDataRange b) throws IOException {
		FileByteChangesDeque d = read(dataFile, logFile);
		d.add(b);
	}

	public FileByteChanges evict()  throws IOException {
		Path least = lru[currLeast];
		lru[currLeast] = null;
		FileByteChanges ret = table.remove(least);
		ret.writeLog();
		currLeast = (currLeast + 1) % size;
		return ret;
	}

	public void tableFlush() throws IOException {
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
