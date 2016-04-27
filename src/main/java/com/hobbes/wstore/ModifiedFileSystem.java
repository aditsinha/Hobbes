package com.hobbes.wstore;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;

public class ModifiedFileSystem {
	private FileSystem fs;
	private static Path blockChangesLogPath;
	private static Path byteChangesLogPath;

	public ModifiedFileSystem() { }

	public static ModifiedFileSystem get() {
		fs = FileSystemFactory.get();
	}

	public ModifiedOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize) throws IOException{
		FSDataOutputStream fOutput = fs.create(f, overwrite, bufferSize, replication, blockSize);
		ArrayList<Path> logPaths = getFilePaths(f);
		fs.create(logPaths[0]);
		fs.create(logPaths[1]);
		handler = FileChangesHandlerCoordinator.get(f, logPaths[0], logPaths[1]);
		return new ModifiedOutputStream(fOutput, handler);
	}

	public boolean delete(Path f) throws IOException {
		ArrayList<Path> logPaths = getFilePaths(f);
		boolean mainDel = fs.delete(f);
		boolean blockDel = fs.delete(logPaths[0]);
		boolean byteDel = fs.delete(logPaths[1]);
		return (mainDel && byteDel && blockDel)
	}

	public ModifiedInputStream open(Path f) throws IOException {
		FSDataInputStream fInput = fs.open(f);
		ArrayList<Path> logPaths = getFilePaths(f);
		blockChangesLogPath = logPaths[0];
		byteChangesLogPath = logPaths[1];
		handler = FileChangesHandlerCoordinator.get(f, logPaths[0], logPaths[1]);
		return new ModifiedInputStream(fInput, handler);
	}

	public void close() throws IOException {
		fs.close()
	}

	// // modification to append
	// public ModifiedOutputStream write() throws IOException{
		
	// }

	private ArrayList<Path> getFilePaths(Path f) throws IOException {
		String fName = f.getName();
		Path parent = f.getParent();
		Path blockChangesLogPath = new Path(parent.toString() + "/.logblock-" + fName);
		Path byteChangesLogPath = new Path(parent.toString() + "/.logbyte-" + fName);
		ArrayList<Path> al= new ArrayList<Path>();
		al.add(blockChangesLogPath);
		al.add(byteChangesLogPath);
		return al;
	}

}