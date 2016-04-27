package com.hobbes.wstore;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;

public class ModifiedOutputStream {

	List<ByteArrayDataRange> changes;
	private static FileChangesHandler handler;
	
	public ModifiedOutputStream (FileChangesHandler handler) {
		this.handler = handler;
	}

	public int write (int startPosition, byte[] buf, int length) throws IOException {
		ByteArrayDataRange change = new ByteArrayDataRange(startPosition, startPosition+length, buf);
		changes.add(change);
		return length;		
	}

	public void hflush() throws IOException {
		handler.write(changes);
		changes.clear();
	}

	public void hsync() throws IOException {
		handler.sync();
	}
}
