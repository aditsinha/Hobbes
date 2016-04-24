package com.hobbes.wstore;  

import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

class FileSystemFactory {
    private static Configuration conf;

    private FileSystemFactory() { }
    
    public static void configure(String[] args) throws IOException {
		Configuration tempConf = new Configuration();
		GenericOptionsParser parser = new GenericOptionsParser(tempConf, args);
		conf = tempConf;
    }
	
	public static FileSystem get() throws IOException {
		if(conf == null) {
			configure(new String[]{});
		}
		return FileSystem.get(conf);
	}

}
