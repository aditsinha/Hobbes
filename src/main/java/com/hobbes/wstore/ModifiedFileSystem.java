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
		ArrayList<Path> logPaths = getFilePaths(f);
		fs.create(logPaths.get(0));
		fs.create(logPaths.get(1));
		FileChangesHandler handler = FileChangesHandlerCoordinator.getInstance().get(f, logPaths.get(0), logPaths.get(1));
		return new ModifiedOutputStream(handler);
	}

	public boolean delete(Path f) throws IOException {
		ArrayList<Path> logPaths = getFilePaths(f);
		boolean mainDel = fs.delete(f);
		boolean blockDel = fs.delete(logPaths.get(0));
		boolean byteDel = fs.delete(logPaths.get(1));
		return (mainDel && byteDel && blockDel);
	}

	public ModifiedInputStream open(Path f) throws IOException {
		fs.open(f);
		ArrayList<Path> logPaths = getFilePaths(f);
		Path blockChangesLogPath = logPaths.get(0);
		Path byteChangesLogPath = logPaths.get(1);
		FileChangesHandler handler = FileChangesHandlerCoordinator.getInstance().get(f, logPaths.get(0), logPaths.get(1));
		return new ModifiedInputStream(handler);
	}

	public void close() throws IOException {
		fs.close();
	}

	// modification to append
	public ModifiedOutputStream write(Path f) throws IOException{
		ArrayList<Path> logPaths = getFilePaths(f);
		Path blockChangesLogPath = logPaths.get(0);
		Path byteChangesLogPath = logPaths.get(1);
		FileChangesHandler handler = FileChangesHandlerCoordinator.getInstance().get(f, logPaths.get(0), logPaths.get(1));
		return new ModifiedOutputStream(handler);
	}

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
