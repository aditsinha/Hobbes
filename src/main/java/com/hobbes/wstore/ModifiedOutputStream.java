package com.hobbes.wstore;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;

public class ModifiedOutputStream {

	List<ByteArrayDataRange> changes;
	private static FileChangesHandler handler;
	private static FSDataOutputStream outputStream;
	
	public ModifiedOutputStream (FSDataOutputStream outputStream, FileChangesHandler handler) {
		this.outputStream = outputStream;
		this.handler = handler;
	}

	public void hflush() throws IOException{
		FileChangesHandler fch.write(changes);
		changes.clear();
	}

	// Still waiting for Adit to expose this feature
	public void hsync() {

	}
}