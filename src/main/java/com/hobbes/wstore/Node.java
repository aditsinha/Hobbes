package com.hobbes.wstore;

/**
 * Hello world!
 *
 */
public class Node {
	int beginBlock;
	int beginByte;
	int endBlock;
	int endByte;
	Node next;

	public Node(int beginBlock, int beginByte, int endBlock, int endByte) {
		this.beginBlock = beginBlock;
		
		this.next = null;
	}

	
}
