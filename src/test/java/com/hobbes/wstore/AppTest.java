package com.hobbes.wstore;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.fs.*;

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

    /**
     * Rigourous Test :-)
     */
    public void testApp() throws Exception
    {
	FileSystem fs = FileSystemFactory.get();
	//assertTrue(fs.exists(new Path("/home/ubuntu/hobbes-chris/h")));
	FSDataOutputStream out = fs.create(new Path("hello"));
	out.writeUTF("Hello World!!\n");
	out.hsync();
	out.close();

	//FSDataInputStream in = fs.read(new Path("/home/ubuntu/hobbes-chris/hi"));
	assertEquals(0, fs.getFileStatus(new Path("hello")).getLen());
    }
}
