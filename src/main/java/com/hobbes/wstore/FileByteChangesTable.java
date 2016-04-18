package com.hobbes.wstore;
import java.util.*;
import java.util.*;

import org.apache.hadoop.fs.*;

public class FileByteChangesCoordinator {
  	private Map<Path, FileByteChanges> table;
	private int size;
	private FileSystem fileSystem;

	public FileByteChangesCoordinator(FileSystem fileSystem, int size) {
		table = new HashMap<Path, FileByteChanges>();
		this.size = size;
		this.fileSystem = fileSystem;
	}

	public FileByteChanges insert(Path dataFile, Path logFile) {
		FileByteChanges fbc = new FileByteChangesDeque(fileSystem, dataFile, logFile);
		table.put(dataFile, fbc);
		if(t.size() > size) {
			evict();
		}
		return fbc;
	}

	public boolean check(Path dataFile) {
		return (table.containsKey(dataFile));
	}
	
	public FileByteChangesDeque read(Path dataFile, Path logFile) {
		if(!check(dataFile)) {
			insert(dataFile, logFile);
		}
		FileByteChangesDeque ret = table.get(dataFile).getDeque();
		return ret;
	}
	
	public void write(Path dataFile, Path logFile, ByteArrayDataRange b) {
		FileByteChangesDeque d = read(dataFile, logFile);
		d.add(b);
	}
}
