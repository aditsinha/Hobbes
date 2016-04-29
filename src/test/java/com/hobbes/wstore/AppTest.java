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

    private static int ONE_MB = 1048576;
    private static byte[] getMbBlock(byte val) {
	byte[] ret = new byte[ONE_MB];
	Arrays.fill(ret, val);
	return ret;
    }

    private static byte[] getHalfMbBlock(byte val) {
	byte[] ret = new byte[ONE_MB / 2];
	Arrays.fill(ret, val);
	return ret;
    }

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
		
		Path path = new Path("chris");

		FileSystem fs = FileSystemFactory.get();
		short replication = 3;
		long blockSize = 1048576;

		ModifiedFileSystem mfs = new ModifiedFileSystem(fs);
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


    public void testReadInBlockLog()
    {
	try {
	    Path logPath = new Path("blockLog");
	    Path dataPath = new Path("dataPath");
	    FileSystem fs = FileSystemFactory.get();

	    FSDataOutputStream logOut = fs.create(logPath);

	    logOut.writeChar('b');
	    logOut.writeLong(0);
	    logOut.writeLong(0);
	    logOut.writeChar('b');
	    logOut.writeLong(1);
	    logOut.writeLong(1);
	    logOut.writeChar('s');
	    logOut.writeLong(2 * ONE_MB);

	    logOut.close();

	    FSDataOutputStream dataOut = fs.create(dataPath, true, ONE_MB, (short) 1, ONE_MB);
	    dataOut.write(getMbBlock((byte) 0), 0, ONE_MB);
	    dataOut.write(getMbBlock((byte) 1), 0, ONE_MB);
	    dataOut.close();

	    FileBlockChanges fbc = new FileBlockChanges(fs, dataPath, logPath);

	    FileByteChangesDeque fbcd = new FileByteChangesDeque(dataPath);
	    
	    fbcd.add(new ByteArrayDataRange(0, ONE_MB, getBlock((byte) 2, ONE_MB)));

	    fbc.incorporateChanges(fbcd);

	    System.out.println("hi");

	    List<DataRange> resolved = fbc.resolve(0, 2*ONE_MB);
	    assertEquals(2, resolved.size());
	    byte[] data = new byte[2*ONE_MB];

	    resolved.get(0).getData(0, data, 0, ONE_MB);
	    resolved.get(1).getData(0, data, ONE_MB, ONE_MB);

	    for (int i = 0; i < ONE_MB; i++) {
		assertEquals(2, data[i]);
	    }

	    System.out.println("bye");
	    
	    for (int i = ONE_MB; i < 2*ONE_MB; i++) {
		assertEquals(1, data[i]);
	    }

	} catch (Exception e) {
	    e.printStackTrace();
	}
    }*/
}
