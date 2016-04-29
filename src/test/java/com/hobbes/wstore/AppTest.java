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
    public void test1() {
	Path path = new Path("test1");
	try {
	    ModifiedFileSystem mfs = ModifiedFileSystem.get();
	    ModifiedOutputStream mos = mfs.create(path, true, ONE_MB, (short)1, ONE_MB);
	    mos.write(0, getBlock((byte)0, ONE_KB), ONE_KB);
	    mos.write(ONE_KB, getBlock((byte)1, ONE_KB), ONE_KB);
	    mos.write(2*ONE_KB, getBlock((byte)2, ONE_KB/2), ONE_KB/2);

	    mos.hflush();

	    mos.write(0, getBlock((byte)3, ONE_KB/2), ONE_KB/2);

	    mos.hflush();

	    mos.hsync();

	    ModifiedInputStream mis = mfs.open(path);
	    byte[] data = new byte[5*ONE_KB/2];
	    mis.read(0, data, 0, 5*ONE_KB/2);

	    assertEquals(3, data[0]);
	    assertEquals(0, data[ONE_KB/2]);
	    assertEquals(1, data[ONE_KB]);
	    assertEquals(2, data[2*ONE_KB]);
	} catch (Throwable t) {
	    t.printStackTrace();
	}
    }
}
