package com.hobbes.wstore;
import java.util.*;

public class FileByteChangesTable {
  	private Map<String, Deque> table;

	public FileByteChangesTable() {
		table = new HashMap<String, Deque>();
	}

	public Deque insert(String filename) {
		Deque empty = new Deque();
		table.put(filename, empty);
		return empty;
	}

	public boolean check(String filename) {
		if(table.containsKey(filename)) return true;
		return false;
	}
	
	public Deque retrieve(String filename) {
		if(!check(filename)) {
			return null;
		}
		Deque ret = table.get(filename);
		return ret;
	}
}
