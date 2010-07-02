package com.bigdata.quorum.zk;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

public class TestSetDifference extends TestCase {
	
	public void testUnorderedSetDifference() {
		String[] old = {"1","2","3","4"};
		String[] fut = {"3","1","5","6"};
		
		UnorderedSetDifference<String> usd = 
			new UnorderedSetDifference<String>(old, fut);
		
		String[] added = {"5","6"};
		assertEquals(usd.added(), toList(added));
		String[] removed = {"2","4"};
		assertEquals(usd.removed(), toList(removed));
	}

	
	public void testOrderedSetDifference() {
		String[] old = {"1","2","3","4","5"};
		String[] fut = {"3","5","1","6"};
		
		OrderedSetDifference<String> osd = 
			new OrderedSetDifference<String>(old, fut);
		
		String[] added = {"1","6"};
		assertEquals(osd.added(), toList(added));
		String[] removed = {"1","2","4"};
		assertEquals(osd.removed(), toList(removed));
	}

	private List<String> toList(String[] arr) {
		ArrayList<String> ret = new ArrayList<String>();
		
		for (int i = 0; i < arr.length; i++) {
			ret.add(arr[i]);
		}
		
		return ret;
	}

}
