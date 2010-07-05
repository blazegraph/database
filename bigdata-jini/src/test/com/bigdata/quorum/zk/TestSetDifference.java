package com.bigdata.quorum.zk;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

/**
 * Test suite for {@link OrderedSetDifference} and {@link UnorderedSetDifference}.
 * 
 * @author Martyn Cutcher
 * @version $Id$
 */
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

    public void testOrderedSetDifference2() {
        String[] old = {"a","b","c"};
        String[] fut = {"c","d","a"};
        
        OrderedSetDifference<String> osd = 
            new OrderedSetDifference<String>(old, fut);
        
        String[] added = {"d","a"};
        assertEquals(osd.added(), toList(added));
        String[] removed = {"a","b"};
        assertEquals(osd.removed(), toList(removed));
    }

    public void testOrderedSetDifference3() {
        String[] old = {"a"};
        String[] fut = {};
        
        OrderedSetDifference<String> osd = 
            new OrderedSetDifference<String>(old, fut);
        
        String[] added = {};
        assertEquals(osd.added(), toList(added));
        String[] removed = {"a"};
        assertEquals(osd.removed(), toList(removed));
    }

    public void testOrderedSetDifference4() {
        String[] old = {};
        String[] fut = {"a"};
        
        OrderedSetDifference<String> osd = 
            new OrderedSetDifference<String>(old, fut);
        
        String[] added = {"a"};
        assertEquals(osd.added(), toList(added));
        String[] removed = {};
        assertEquals(osd.removed(), toList(removed));
    }

	private List<String> toList(final String[] arr) {
		
	    final ArrayList<String> ret = new ArrayList<String>();
		
		for (int i = 0; i < arr.length; i++) {
			ret.add(arr[i]);
		}
		
		return ret;
	}

}
