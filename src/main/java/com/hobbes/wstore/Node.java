package com.hobbes.wstore;

/**
 * Hello world!
 *
 */
public class Node {
	ByteArrayDataRange range;
	Node next;

	public Node(ByteArrayDataRange range) {
		this.range = range;
		this.next = null;
	}

	
}
