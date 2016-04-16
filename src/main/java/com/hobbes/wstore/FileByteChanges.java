package com.hobbes.wstore;

public class FileByteChanges {

	private FileByteChangesTable t;

	public FileByteChanges() {
		FileByteChangesTable t = new FileByteChangesTable();
	}

	public Deque read(String filename) {
		return t.retrieve(filename);
	}

	public void write(String filename, ByteArrayDataRange b) {
		Deque d;
		if(!t.check(filename)) {
			d = t.insert(filename);
		} else {
			d = t.retrieve(filename);
		}
		d.add(b);
	}
}
