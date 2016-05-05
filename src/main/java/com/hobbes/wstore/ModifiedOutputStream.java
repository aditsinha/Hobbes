package com.hobbes.wstore;

import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;

public class ModifiedOutputStream extends OutputStream {

    List<ByteArrayDataRange> changes = new ArrayList<>();
    private FileChangesHandler handler;
    private long seekPosition = 0;
	private long numChanges = 0;
	
    public ModifiedOutputStream (FileChangesHandler handler) {
	this.handler = handler;
    }

    public int write (long startPosition, byte[] buf, int offset, int length) throws IOException {
	if (offset != 0) {
	    byte[] newBuf = new byte[length];
	    System.arraycopy(buf, offset, newBuf, 0, length);
	    buf = newBuf;
	}

	ByteArrayDataRange change = new ByteArrayDataRange(startPosition, startPosition+length, buf);
	changes.add(change);
	numChanges += length;
	if (numChanges > 50000) {
		hflush();
		numChanges = 0;
	}
	return length;
    }

    @Override
    public void close() throws IOException {
	hsync();
	FileChangesHandlerCoordinator fchc = FileChangesHandlerCoordinator.getInstance();
	fchc.unget(handler);
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

    @Override
    public void flush() throws IOException {
	hsync();
    }

    @Override
    public void write(byte[] b) throws IOException {
	write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
	write(seekPosition, b, off, len);
	seekPosition += len;
    }

    @Override
    public void write(int b) throws IOException {
	write(new byte[] {(byte)b}, 0, 1);
    }
}
