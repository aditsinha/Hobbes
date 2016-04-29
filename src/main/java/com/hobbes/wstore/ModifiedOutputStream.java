package com.hobbes.wstore;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;

public class ModifiedOutputStream {

	List<ByteArrayDataRange> changes;
	private static FileChangesHandler handler;
	private long seekPosition = 0;
	
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

	// check if its a seekable position and sets it
	public boolean seek (long position) throws IOException {
		long fileSize = handler.getLastLogicalPosition();
		if (position > fileSize) {
			return false;
		}
		seekPosition = position;
		return true;
	}
}
