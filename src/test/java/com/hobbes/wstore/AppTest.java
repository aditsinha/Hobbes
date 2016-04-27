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
	Path data = new Path("/home/ubuntu/hobbes-chris/asdf");
	Path path = new Path("/home/ubuntu/hobbes-chris/asdf.out");
	FileSystem fs = FileSystemFactory.get();

	FSDataOutputStream dataOut = fs.create(data);
	FSDataOutputStream out = fs.create(path);

	/*if(fs.exists(data)) {
		dataOut = fs.append(data);
	} else {
		dataOut = fs.create(data);
	}*/

	dataOut.writeBytes("hellllllo world");
	dataOut.close();

	/*if (fs.exists(path)) {
	    out = fs.append(path);
	} else {
	    out = fs.create(path);
	}*/

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
	//assertEquals(fbc.getDeque().getDeque().size(), 2);
	fbc.getDeque().print();

	/*FSDataInputStream in = fs.open(path);
	BufferedReader reader = new BufferedReader(new InputStreamReader(in));

	
	String line;
	int lineCount = 0;
	while ((line = reader.readLine()) != null) {
	    assertEquals("Hello World!!", line);
	    lineCount++;
	}
	System.out.println("Line Count: " + lineCount);*/
    }
}
