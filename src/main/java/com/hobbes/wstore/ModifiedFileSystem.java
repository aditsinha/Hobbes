package com.hobbes.wstore;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;

public class ModifiedFileSystem {
	private FileSystem fs;

	protected ModifiedFileSystem(FileSystem fs) {
		this.fs = fs;

	}

	public static ModifiedFileSystem get() throws IOException {
		return new ModifiedFileSystem(FileSystemFactory.get());
	}

	public ModifiedOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize) throws IOException{
		fs.create(f, overwrite, bufferSize, replication, blockSize);
		HashMap<String, Path> logPaths = getFilePaths(f);
		fs.create(logPaths.get("blockChangesLogPath"));
		fs.create(logPaths.get("byteChangesLogPath"));
		FileChangesHandler handler = FileChangesHandlerCoordinator.getInstance().get(f, blockChangesLogPath, byteChangesLogPath);
		return new ModifiedOutputStream(handler);
	}

	public boolean delete(Path f) throws IOException {
		HashMap<String, Path> logPaths = getFilePaths(f);
		boolean mainDel = fs.delete(f);
		boolean blockDel = fs.delete(logPaths.get("blockChangesLogPath"));
		boolean byteDel = fs.delete(logPaths.get("byteChangesLogPath"));
		return (mainDel && byteDel && blockDel);
	}

	public ModifiedInputStream open(Path f) throws IOException {
		fs.open(f);
		HashMap<String, Path> logPaths = getFilePaths(f);
		Path blockChangesLogPath = logPaths.get("blockChangesLogPath");
		Path byteChangesLogPath = logPaths.get("byteChangesLogPath");
		FileChangesHandler handler = FileChangesHandlerCoordinator.getInstance().get(f, blockChangesLogPath, byteChangesLogPath);
		return new ModifiedInputStream(handler);
	}

	public void close() throws IOException {
		fs.close();
	}

	// modification to append
	public ModifiedOutputStream write(Path f) throws IOException{
		HashMap<String, Path> logPaths = getFilePaths(f);
		Path blockChangesLogPath = logPaths.get("blockChangesLogPath");
		Path byteChangesLogPath = logPaths.get("byteChangesLogPath");
		FileChangesHandler handler = FileChangesHandlerCoordinator.getInstance().get(f, blockChangesLogPath, byteChangesLogPath);
		return new ModifiedOutputStream(handler);
	}

	private HashMap<String, Path> getFilePaths(Path f) throws IOException {
		String fName = f.getName();
		Path parent = f.getParent();
		Path blockChangesLogPath = new Path(parent.toString() + "/.logblock-" + fName);
		Path byteChangesLogPath = new Path(parent.toString() + "/.logbyte-" + fName);
		HashMap<String, Path> hmap = new HashMap<String, Path>();
		hmap.put("blockChangesLogPath", blockChangesLogPath);
		hmap.put("byteChangesLogPath", byteChangesLogPath);
		return hmap;
	}

}
