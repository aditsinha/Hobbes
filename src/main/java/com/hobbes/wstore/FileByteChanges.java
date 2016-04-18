package com.hobbes.wstore;

import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;

public class FileByteChanges {

	private FileByteChangesDeque d;

	private final long blockSize;

	private FSDataOutputStream logOut;

	public FileByteChanges(FileSystem fileSystem, Path dataFile, Path logFile) throws IOException {
		FileStatus status = fileSystem.getFileStatus(dataFile);

		this.blockSize = status.getBlockSize();
		this.logOut = fileSystem.append(logFile);
		
		FileByteChangesTable d = new FileByteChangesDeque(dataFile);
		
		FSDataInputStream logIn = fileSystem.open(logFile);
		readLog(logIn);
		
	}

	public FileByteChangesDeque getDeque() {
		return d;
	}

	public void readLog(DataInputStream logIn) {
		try {
			while(true) {
				long logicalStartPosition = logIn.readLong();
				long logicalEndPosition = logIn.readLong();
				long length = logicalStartPosition - logicalEndPosition;
				byte[] backing = new byte[length];
				logIn.read(backing, 0, length);
				ByteArrayDataRange b = new ByteArrayDataRange(logicalStartPosition, logicalEndPosition, backing);
				d.add(b);
				char check = logIn.readChar();
				assert(check == ':');
			}
		} catch (EOFException e1) {
			// done
		}
	}

	
	public void writeLog() {
		ByteBuffer b = ByteBuffer.allocate((int)bockSize);
		byte[] flush;

		for(int i=0; i < t.size(); i++) {
			b.putLong(b.get(i).getLogicalStartPosition());
			b.putLong(b.get(i).getLogicalEndPosition());
			b.put(b.get(i).backing);
			b.putChar(':');
		}

		flush = b.array();

		logOut.write(flush, 0, (int)blockSize);
		logOut.hsync();
	}
}
