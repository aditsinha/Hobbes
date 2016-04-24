package com.hobbes.wstore;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;

public class ModifiedFileSystem {
	private FileSystem fs;
	// cordinator gives the handlers which are used output and input streams
	private static FileChangesHandler handler;

	public ModifiedFileSystem() { }

	public static ModifiedFileSystem get() {
		fs = FileSystemFactory.get();
	}

	public create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize) {
		String fName = f.getName();
		Path parent = f.getParent();
		fs.create(f)
		fs.create(new Path(parent.toString() + ))
		handler = FileChangesHandlerCoordinator.get(f, blockChangesLogPath, byteChangesLogPath);
	}

	public delete() {

	}

	public open () {

	}

	public write(Path f, int bufferSize) {

	}

	public void close() throws IOException {
		fs.close()
	}

}