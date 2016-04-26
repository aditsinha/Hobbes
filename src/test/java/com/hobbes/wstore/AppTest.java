package com.hobbes.wstore;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.fs.*;
import java.io.*;

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
	Path path = new Path("hello");
	FileSystem fs = FileSystemFactory.get();

	FSDataOutputStream out;

	if (fs.exists(path)) {
	    out = fs.append(path);
	} else {
	    out = fs.create(path);
	}

	out.writeBytes("Hello World!!\n");
	out.close();

	FSDataInputStream in = fs.open(path);
	BufferedReader reader = new BufferedReader(new InputStreamReader(in));

	String line;
	int lineCount = 0;
	while ((line = reader.readLine()) != null) {
	    assertEquals("Hello World!!", line);
	    lineCount++;
	}
	System.out.println("Line Count: " + lineCount);
    }
}
