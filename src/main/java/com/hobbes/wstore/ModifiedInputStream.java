package com.hobbes.wstore;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;

public class ModifiedOutputStream {

	private static FileChangesHandler handler;
	private static FSDataInputStream inputStream;

	public ModifiedInputStream (FSDataInputStream inputStream, FileChangesHandler handler) {
		this.inputStream = inputStream;
		this.handler = handler;
	}
	
	public ByteBuffer read(long position, byte[] buffer, int offset, int length) throws IOException{
		// get the filechanges handler
		List<DataRange> data = handler.read(position, length);

		// int 
		// for (DataRange temp : data) {
		// 	temp.getData(0, buffer, int pos, int len);
		// }
		return buffer;
	}

}