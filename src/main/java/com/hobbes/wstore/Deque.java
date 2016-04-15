package com.hobbes.wstore;

/**
 * Hello world!
 *
 */
public class Deque {
	String filename;
	Node head;
	Node tail;
	long size;

	public Node(String filename, Node head) {
		this.filename = filename;
		this.head = head;
		this.tail = head;
		this.size = 1;
	}

	public Node(String filename) {
		this.filename = filename;
		this.head = null;
		this.size = 0;
	}

	public void merge(Node n1, Node n2) {
		if(n1.beginByte <= n.beginByte & n1.endByte >= n.endByte) {
			/* write */
			return;
		} else if(n1.beginByte <= n2.beginByte && n1.endByte <= n2.endByte) {
			n1.endByte = n2.endByte;
			/* write */
		} else if(n1.beginByte >= n2.beginByte && n1.endByte >= n2.endByte) {
			n1.beginByte = n2.beginByte;
			/* write */
		} else if(n1.beginByte >= n.beginByte && n1.endByte <= n2.endByte) {
			n1.beginByte = n2.beginByte;
			n1.endByte = n2.endByte;
			/* write */
		}
	}

	public void add(Node n) {
		Node curr = head;
		while(curr.next != null && curr.beginBlock < n.beginBlock) {
			curr = curr.next;
		}

		if(curr.beginBlock == n.beginBlock) {
			while(curr.next != null && curr.beginByte < n.beginByte) {
				curr = curr.next;
			}
			
			if(curr.beginByte <= n.beginByte & curr.endByte >= n.endByte) {
				/* write */
				return;
			} else if(curr.beginByte <= n.beginByte && curr.endByte <= n.endByte) {
				curr.endByte = n.endByte;
				/* write */
			} else if(curr.beginByte >= n.beginByte && curr.endByte >= n.endByte) {
				curr.beginByte = n.beginByte;
				/* write */
			} else if(curr.beginByte >= n.beginByte && curr.endByte <= n.endByte) {
				curr.beginByte = n.beginByte;
				curr.endByte = n.endByte;
				/* write */
			} else if (curr.beginByte > n.endByte) {
				Node temp = curr.prev;
				temp.next = n;
				n.next = curr;
			} else if(curr.endByte < n.beginByte) {
				Node temp = curr.next;
				curr.next = n;
				n.next = temp;
			}
		} else {
			Node temp = curr.next;
			curr.next = n;
			n.next = temp;
		}
	}


		
}
