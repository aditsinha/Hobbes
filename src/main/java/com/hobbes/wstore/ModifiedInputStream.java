package com.hobbes.wstore;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;

public class ModifiedInputStream extends InputStream {

	private static FileChangesHandler handler;
	private long seekPosition = 0;
	private long markPosition = 0;
	private int currRead = 0;
	private int readLimit = 0;
	private boolean markSupport = false;

	public ModifiedInputStream (FileChangesHandler handler) {
		this.handler = handler;
	}
	
	public long read(long position, byte[] buffer, int offset, int length) throws IOException{
		List<DataRange> data = handler.read(position, length);

		int bytesReadSoFar = 0;
		for (DataRange temp : data) {
			int bytesRead = temp.getData(0, buffer, bytesReadSoFar, (int)temp.size());
			bytesReadSoFar += bytesRead;
		}

		return bytesReadSoFar;
	}

	public int read() throws IOException {
		byte[] buffer = new byte[1];
		long successfulRead = read(seekPosition, buffer, 0, 1);
		if(successfulRead != -1) {
			seekPosition++;
			currRead++;
			if(markSupport && currRead > readLimit) {
				markSupport = false;
				markPosition = -1;
			}
		}
		return (int)buffer[0];
	}

	public int read(byte[] buffer) throws IOException {
		int length = buffer.length;
		int successfulRead = (int)(read(seekPosition, buffer, 0, length));
		if(successfulRead != -1) {
			seekPosition += successfulRead;
			currRead += successfulRead;
			if(markSupport && currRead > readLimit) {
				markSupport = false;
				markPosition = -1;
			}
		}
		return successfulRead;
	}

	public int read(byte[] buffer, int offset, int length) throws IOException {
		int successfulRead = (int)(read(seekPosition, buffer, offset, length));
		if(successfulRead != -1) {
			seekPosition += successfulRead;
			currRead += successfulRead;
			if(markSupport && currRead > readLimit) {
				markSupport = false;
				markPosition = -1;
			}
		}
		return successfulRead;
	}

	public void close() throws IOException {
		FileChangesHandlerCoordinator fchc = FileChangesHandlerCoordinator.getInstance();
		fchc.unget(handler);
	}

    public int available() throws IOException {
	long longAvailable = handler.getLastLogicalPosition() - seekPosition;
	if (longAvailable > Integer.MAX_VALUE) {
	    return Integer.MAX_VALUE;
	}

	return (int) longAvailable;
    }

	public long skip(long n) throws IOException {
		byte[] buffer = new byte[(int) n];
		long successfulRead = read(seekPosition, buffer, 0, (int) n);
		if(successfulRead != -1) seekPosition += successfulRead;
		return successfulRead;
	}

	public boolean markSupported() {
		return markSupport;
	}

	public void mark(int readlimit) {
		readLimit = readlimit;
		currRead = 0;
		markPosition = seekPosition;
	}

	public void reset() {
		currRead = 0;
		markPosition = seekPosition;
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
