package com.hobbes.wstore;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;

public class ModifiedInputStream {

	private static FileChangesHandler handler;

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

	// pass logical start
	// save that value in an instance
	// public seek () {

	// }

}
