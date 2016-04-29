package com.hobbes.wstore;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.fs.*;
import java.io.*;
import java.util.*;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    private static final int ONE_KB = 1024;
    private static final int ONE_MB = ONE_KB * ONE_KB;

    private static byte[] getBlock(byte val, int size) {
	byte[] ret = new byte[size];
	Arrays.fill(ret, val);
	return ret;
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp() throws Exception
    {
	try {	
		Path path = new Path("chris");

		FileSystem fs = FileSystemFactory.get();
		short replication = 3;
		long blockSize = 1048576;

		FileChangesHandlerCoordinator fchc = FileChangesHandlerCoordinator.getInstance();

		ModifiedFileSystem mfs = new ModifiedFileSystem(fs, fchc);
		ModifiedOutputStream mos = mfs.create(path, true, 1000, replication, blockSize);
		String write = "Hello World!!";
		byte[] writeBytes = write.getBytes();
		System.out.println(Integer.toString(writeBytes.length));
		mos.write(0, writeBytes, writeBytes.length);
		mos.close();

		ModifiedInputStream mis = mfs.open(path);
		byte[] readBytes = new byte[writeBytes.length];
		mis.read(0, readBytes, 0, writeBytes.length);
		System.out.println(new String(readBytes));
		} catch (Exception e) {
		 e.printStackTrace();
		 }
/*
		
*/		
    }

	/*public void testByteLog() {
		Path data = new Path("/home/ubuntu/hobbes-chris/asdf");
		Path path = new Path("/home/ubuntu/hobbes-chris/asdf.out");
		FileSystem fs = FileSystemFactory.get();
	
		FSDataOutputStream dataOut = fs.create(data);
		FSDataOutputStream out = fs.create(path);
	
		dataOut.writeBytes("hellllllo world");
		dataOut.close();
	
		out.writeLong(0);
		out.writeLong(6);
		out.writeBytes("hello!");
		out.writeChar(':');
		out.writeLong(6);
		out.writeLong(12);
		out.writeBytes("world!");
		out.writeChar(':');
		out.close();
	
		FileByteChanges fbc = new FileByteChanges(fs, data, path);
		fbc.getDeque().print();
	
		
	}
    */
}
