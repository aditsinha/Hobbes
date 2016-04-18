package com.hobbes.wstore;
import java.util.*;
import java.io.*;

public class FileByteChangesDeque  {
	public static int safeLongToInt(long l) {
		if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
		    throw new IllegalArgumentException
		        (l + " cannot be cast to int without changing its value.");
		}
		return (int) l;
	}

	private ArrayList<ByteArrayDataRange> deque;
	private Path dataFile;

	public FileByteChangesDeque(Path dataFile) {
		this.dataFile = dataFile;
		this.deque = new ArrayList<ByteArrayDataRange>();
	}

	public String getFilename() {
		return filename;
	}


	public void merge(ByteArrayDataRange b1, ByteArrayDataRange b2) {
		long b1Start = b1.getLogicalStartPosition();
		long b1End = b1.getLogicalEndPosition();
		long b2Start = b2.getLogicalStartPosition();
		long b2End = b2.getLogicalEndPosition();
		int srcPos, destPos, length;
		srcPos = destPos = 0;

		/* First case: b1 is [===========================]
					   b2 is        [===========] */
		if(b1Start <= b2Start  &&
		   b1End >= b2End) {
		   	byte[] buf = new byte[safeLongToInt(b1.getLogicalEndPosition() - b1.getLogicalStartPosition())];

			length = safeLongToInt(b2Start-b1Start);

			System.arraycopy(b1.backing, srcPos, buf, destPos, length);

			destPos = safeLongToInt(b2Start-b1Start);
			length = b2.backing.length;

			System.arraycopy(b2.backing, srcPos, buf, destPos, length);
			
			srcPos = destPos = safeLongToInt(b2End-b1Start);
			length = safeLongToInt(b1End-b2End);

			System.arraycopy(b1.backing, srcPos, buf, destPos, length);
			b1.setBacking(buf);

		/* Second case: b1 is [=========================]
						b2 is              [=================] */
		} else if(b1Start <= b2Start && 
				  b1End <= b2End) {
			b1.setLogicalEndPosition(b2End);
			byte[] buf = new byte[safeLongToInt(b1.getLogicalEndPosition() - b1.getLogicalStartPosition())];

			length = safeLongToInt(b2Start-b1Start);

			System.arraycopy(b1.backing, srcPos, buf, destPos, length);

			destPos = safeLongToInt(b2Start-b1Start);
			length = b2.backing.length;

			System.arraycopy(b2.backing, srcPos, buf, destPos, length);
			b1.setBacking(buf);

		/* Third case: b1 is           [==================]
					   b2 is [================] */
		} else if(b1Start >= b2Start && 
				  b1End >= b2End) {
			b1.setLogicalStartPosition(b2Start);
			byte[] buf = new byte[safeLongToInt(b1.getLogicalEndPosition() - b1.getLogicalStartPosition())];

			length = b2.backing.length;

			System.arraycopy(b2.backing, srcPos, buf, destPos, length);

			srcPos = b2.backing.length;
			destPos = safeLongToInt(b2End-b1Start);
			length = safeLongToInt(b1End-b2End);

			System.arraycopy(b1.backing, srcPos, buf, destPos, length);
			b1.setBacking(buf);
		/* Fourth case: b1 is        [=============]
						b2 is   [=========================] */
		} else if(b1Start >= b2Start && 
				  b1End <= b2End) {
			b1.setLogicalStartPosition(b2Start);
			b1.setLogicalEndPosition(b2End);
			b1.setBacking(b2.backing);
		}
	}

	public void add(ByteArrayDataRange b) {
		int size = deque.size();

		long key = b.getLogicalStartPosition();
		int low, mid, high;
		low = mid = 0;
        high = size-1;

        while (low <= high) {
            mid = low + (high - low) / 2;
            if      (key < deque.get(mid).getLogicalStartPosition()) high = mid - 1;
            else if (key > deque.get(mid).getLogicalStartPosition()) low = mid + 1;
        }

		ByteArrayDataRange curr = deque.get(mid);
		if (curr.getLogicalStartPosition() > b.getLogicalEndPosition()) {
			deque.add(mid, b);
		} else if(curr.getLogicalEndPosition() < b.getLogicalStartPosition()) {
			deque.add(mid+1, b);
		} else {
			merge(curr, b);
		}
	}

	public ArrayList<ByteArrayDataRange> getDeque() {
		return deque;
	}

	public void clearDeque() {
		deque = new ArrayList<ByteArrayDataRange>();
	}

	public boolean isEmpty() {
		return deque.size() == 0;
	}
}
