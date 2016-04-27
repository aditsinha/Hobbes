package com.hobbes.wstore;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;

public class ModifiedInputStream {

	private static FileChangesHandler handler;

	public ModifiedInputStream (FileChangesHandler handler) {
		this.handler = handler;
	}
	
	public int read(long position, byte[] buffer, int offset, int length) throws IOException{
		List<DataRange> data = handler.read(position, length);

		// int 
		// for (DataRange temp : data) {
		// 	temp.getData(0, buffer, int pos, int len);
		// }
		// Eventually change this to the number of bytes read
		return 0;
	}

	// public seek () {

	// }

}
