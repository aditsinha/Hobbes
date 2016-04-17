package com.hobbes.wstore;
import java.util.*;

public class FileByteChangesTable {
  	private Map<String, FileByteChangesDeque> table;

	public FileByteChangesTable() {
		table = new HashMap<String, FileByteChangesDeque>();
	}

	public FileByteChangesDeque insert(String filename) {
		FileByteChangesDeque empty = new FileByteChangesDeque();
		table.put(filename, empty);
		return empty;
	}

	public boolean check(String filename) {
		if(table.containsKey(filename)) return true;
		return false;
	}
	
	public FileByteChangesDeque retrieve(String filename) {
		if(!check(filename)) {
			return null;
		}
		FileByteChangesDeque ret = table.get(filename);
		return ret;
	}
}
