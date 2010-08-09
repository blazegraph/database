/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on May 15, 2009
 */

package com.bigdata.cache;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import junit.framework.TestCase2;

/**
 * Test suite for {@link RingBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRingBuffer extends TestCase2 {

    /**
     * 
     */
    public TestRingBuffer() {
   
    }

    /**
     * @param name
     */
    public TestRingBuffer(String name) {
   
        super(name);
   
    }

    public void test_ctor() {
        
        try {
            new RingBuffer<String>(0);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring excepted exception: " + ex);
        }
        
        try {
            new RingBuffer<String>(-1);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring excepted exception: " + ex);
        }

        final RingBuffer<String> b = new RingBuffer<String>(1);

        assertEquals("capacity", 1, b.capacity());
        assertEquals("size", 0, b.size());
        assertTrue("isEmpty", b.isEmpty());
        assertFalse("isFull", b.isFull());
        assertNull("peek()", b.peek());
        assertNull("poll()", b.poll());
        try {
            b.remove();
            fail("Expecting: " + NoSuchElementException.class);
        } catch (NoSuchElementException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring excepted exception: " + ex);
        }
        try {
            b.get(0);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring excepted exception: " + ex);
        }
        try {
            b.get(-1);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring excepted exception: " + ex);
        }
        
    }

    /**
     * Tests the basic functionality of the ring buffer by incrementally
     * populating and then draining the buffer, including the case where the
     * ring buffer wraps around.
     * <pre>
     *        : [ _, _, _ ] : head=tail=size=0 (empty; head==tail)
     * add(a) : [ a, _, _ ] : head=1; tail=0; size=1
     * add(b) : [ a, b, _ ] : head=2; tail=0; size=2
     * add(c) : [ a, b, c ] : head=0; tail=0; size=3 (head wrapped, full, head==tail)
     * poll() : [ _, b, c ] : head=0; tail=1; size=2; returns [a].
     * add(d) : [ d, b, c ] : head=1; tail=1; size=3; (full, head==tail) 
     * poll() : [ d, _, c ] : head=1; tail=2; size=2; returns [b].
     * poll() : [ d, _, _ ] : head=1; tail=0; size=1; returns [c] (tail wrapped)
     * poll() : [ _, _, _ ] : head=1; tail=1; size=0; returns [d] (empty, head==tail)
     * </pre>
     */
    public void test_ringBuffer() {

        final String a = "a";
        final String b = "b";
        final String c = "c";
        final String d = "d";
        
        final RingBuffer<String> buffer = new RingBuffer<String>(3);

        assertEquals("capacity", 3, buffer.capacity());

        assertEquals("size", 0, buffer.size());
        assertEquals("head", 0, buffer.getHeadIndex());
        assertEquals("tail", 0, buffer.getTailIndex());
        assertEquals("refs",new String[]{},buffer.toArray(new String[0]));
        assertTrue("isEmpty", buffer.isEmpty());
        assertFalse("isFull", buffer.isFull());
        assertEquals("peek()", null, buffer.peek());

        // add the first element.
        buffer.add(a);

        assertEquals("size", 1, buffer.size());
        assertEquals("head", 1, buffer.getHeadIndex());
        assertEquals("tail", 0, buffer.getTailIndex());
        assertEquals("refs",new String[]{a},buffer.toArray(new String[0]));
        assertFalse("isEmpty", buffer.isEmpty());
        assertFalse("isFull", buffer.isFull());
        assertEquals("peek()", a, buffer.peek());
        assertEquals("get(0)", a, buffer.get(0));

        buffer.add(b);

        assertEquals("size", 2, buffer.size());
        assertEquals("head", 2, buffer.getHeadIndex());
        assertEquals("tail", 0, buffer.getTailIndex());
        assertEquals("refs",new String[]{a,b},buffer.toArray(new String[0]));
        assertFalse("isEmpty", buffer.isEmpty());
        assertFalse("isFull", buffer.isFull());
        assertEquals("peek()", a, buffer.peek());
        assertEquals("get(0)", a, buffer.get(0));
        assertEquals("get(1)", b, buffer.get(1));

        // add another element - the ring buffer is now full (head==tail)
        buffer.add(c);

        assertEquals("size", 3, buffer.size());
        assertEquals("head", 0, buffer.getHeadIndex());
        assertEquals("tail", 0, buffer.getTailIndex());
        assertEquals("refs",new String[]{a,b,c},buffer.toArray(new String[0]));
        assertFalse("isEmpty", buffer.isEmpty());
        assertTrue("isFull", buffer.isFull());
        assertEquals("peek()", a, buffer.peek());
        assertEquals("get(0)", a, buffer.get(0));
        assertEquals("get(1)", b, buffer.get(1));
        assertEquals("get(2)", c, buffer.get(2));

        // verify add(ref) is refused.
        try {
            buffer.add(d);
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring excepted exception: " + ex);
        }

        // and verify that there were no side-effects when it was refused.
        assertEquals("size", 3, buffer.size());
        assertEquals("head", 0, buffer.getHeadIndex());
        assertEquals("tail", 0, buffer.getTailIndex());
        assertEquals("refs",new String[]{a,b,c},buffer.toArray(new String[0]));
        assertFalse("isEmpty", buffer.isEmpty());
        assertTrue("isFull", buffer.isFull());
        assertEquals("peek()", a, buffer.peek());
        assertEquals("get(0)", a, buffer.get(0));
        assertEquals("get(1)", b, buffer.get(1));
        assertEquals("get(2)", c, buffer.get(2));

        // take an element from the buffer.
        assertEquals(a, buffer.poll());
        assertEquals("size", 2, buffer.size());
        assertEquals("head", 0, buffer.getHeadIndex());
        assertEquals("tail", 1, buffer.getTailIndex());
        assertEquals("refs",new String[]{b,c},buffer.toArray(new String[0]));
        assertFalse("isEmpty", buffer.isEmpty());
        assertFalse("isFull", buffer.isFull());
        assertEquals("peek()", b, buffer.peek());
        assertEquals("get(0)", b, buffer.get(0));
        assertEquals("get(1)", c, buffer.get(1));

        // Add another element (the buffer should be full again).
        buffer.add(d);

        assertEquals("size", 3, buffer.size());
        assertEquals("head", 1, buffer.getHeadIndex());
        assertEquals("tail", 1, buffer.getTailIndex());
        assertEquals("refs",new String[]{b,c,d},buffer.toArray(new String[0]));
        assertFalse("isEmpty", buffer.isEmpty());
        assertTrue("isFull", buffer.isFull());
        assertEquals("peek()", b, buffer.peek());
        assertEquals("get(0)", b, buffer.get(0));
        assertEquals("get(1)", c, buffer.get(1));
        assertEquals("get(2)", d, buffer.get(2));
        
        // take an element from the buffer.
        assertEquals(b, buffer.poll());
        assertEquals("size", 2, buffer.size());
        assertEquals("head", 1, buffer.getHeadIndex());
        assertEquals("tail", 2, buffer.getTailIndex());
        assertEquals("refs",new String[]{c,d},buffer.toArray(new String[0]));
        assertFalse("isEmpty", buffer.isEmpty());
        assertFalse("isFull", buffer.isFull());
        assertEquals("peek()", c, buffer.peek());
        assertEquals("get(0)", c, buffer.get(0));
        assertEquals("get(1)", d, buffer.get(1));

        // take an element from the buffer.
        assertEquals(c, buffer.poll());
        assertEquals("size", 1, buffer.size());
        assertEquals("head", 1, buffer.getHeadIndex());
        assertEquals("tail", 0, buffer.getTailIndex());
        assertEquals("refs",new String[]{d},buffer.toArray(new String[0]));
        assertFalse("isEmpty", buffer.isEmpty());
        assertFalse("isFull", buffer.isFull());
        assertEquals("peek()", d, buffer.peek());
        assertEquals("get(0)", d, buffer.get(0));

        // take the last element from the buffer (buffer is empty; head==tail)
        assertEquals(d, buffer.poll());
        assertEquals("size", 0, buffer.size());
        assertEquals("head", 1, buffer.getHeadIndex());
        assertEquals("tail", 1, buffer.getTailIndex());
        assertEquals("refs",new String[]{},buffer.toArray(new String[0]));
        assertTrue("isEmpty", buffer.isEmpty());
        assertFalse("isFull", buffer.isFull());
        assertEquals("peek()", null, buffer.peek());
        try {
            buffer.get(0);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring excepted exception: " + ex);
        }
        
    }

    /**
     * Unit test for the ability to remove an element at any (valid) index in
     * the ring buffer. This operation requires us to close up the hole in the
     * ring buffer and adjust the tail (the next element to be read). Those
     * behaviors are tested here. Note that all of the random access remove
     * operations go through {@link RingBuffer#remove(int)}.
     * <p>
     * The unit tests beings by adding in elements until the buffer is in this
     * state:
     * 
     * <pre>
     *           : [ a, b, c ] : head=0; tail=0; size=3 (full, head==tail)
     * remove(1) : [ a, c, _ ] : head=2; tail=0; size=2, returns [b]
     * remove(1) : [ a, _, _ ] : head=1; tail=0; size=1, returns [c]
     * remove(0) : [ _, _, _ ] : head=0; tail=0; size=0, returns [a] (empty, head==tail)
     * </pre>
     * Note that the last case removed the tail.
     * <p>
     * The following condition is also tested. First, we fill the buffer. Then
     * we remove an element. Finally we add another element. This yields the
     * following state, which is the same state evaluated in
     * {@link #test_ringBuffer()}.
     * 
     * <pre>
     *           : [ d, b, c ] : head=1; tail=1; size=3; (full, head==tail) 
     * remove(1) : [ d, c, _ ] : head=2; tail=1; size=2, returns [b] (remove tail)
     * remove(0) : [ c, _, _ ] : head=x; tail=x; size=1, returns [d]
     * remove(0) : [ _, _, _ ] : head=0; tail=0; size=0, returns [c] (empty, head==tail)
     * </pre>
     * 
     * When removing the tail, head := (head-1) % capacity.
     */
    public void test_removeNth() {

        final String a = "a";
        final String b = "b";
        final String c = "c";
//        final String d = "d";
        
        final RingBuffer<String> buffer = new RingBuffer<String>(3);

        buffer.add(a);
        buffer.add(b);
        buffer.add(c);

        // verify the initial state.
        assertEquals("size", 3, buffer.size());
        assertEquals("head", 0, buffer.getHeadIndex());
        assertEquals("tail", 0, buffer.getTailIndex());
        assertEquals("refs",new String[]{a,b,c},buffer.toArray(new String[0]));

    }

    /**
     * Unit test for the iterator. This iterator IS NOT thread-safe. It visits
     * elements in the order in which they will be read from the buffer (LRU to
     * MRU).
     */
    public void test_iterator() {

        final String a = "a";
        final String b = "b";
        final String c = "c";
        final String d = "d";
        
        final RingBuffer<String> buffer = new RingBuffer<String>(3);

        assertSameIterator(new String[]{}, buffer.iterator());

        buffer.add(a);
        assertSameIterator(new String[]{a}, buffer.iterator());
        
        buffer.add(b);
        assertSameIterator(new String[]{a,b}, buffer.iterator());
        
        buffer.add(c);
        assertSameIterator(new String[]{a,b,c}, buffer.iterator());
        
        assertEquals(a,buffer.remove());
        assertSameIterator(new String[]{b,c}, buffer.iterator());
        
        buffer.add(d);
        assertSameIterator(new String[]{b,c,d}, buffer.iterator());
        
        assertEquals(b,buffer.remove());
        assertSameIterator(new String[]{c,d}, buffer.iterator());

        assertEquals(c,buffer.remove());
        assertSameIterator(new String[]{d}, buffer.iterator());

        assertEquals(d,buffer.remove());
        assertSameIterator(new String[]{}, buffer.iterator());

    }

    /**
     * Unit test for Iterator#remove.
     * <p>
     * We tested the ability to remove elements in {@link #test_removeNth()} so
     * all we have to cover here is the ability of the iterator to remove the
     * correct element and to refuse to (a) double-remove an element; and (b) to
     * remove an element when non-has been visited.
     */
    public void test_iterator_remove() {

        final String a = "a";
        final String b = "b";
        final String c = "c";

        final RingBuffer<String> buffer = new RingBuffer<String>(3);

        buffer.add(a);
        buffer.add(b);
        buffer.add(c);
        assertSameIterator(new String[] { a, b, c }, buffer.iterator());

        // verify remove not allowed before next().
        try {
            buffer.iterator().remove();
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // verify remove of [b].
        {
            final Iterator<String> itr = buffer.iterator();
            assertEquals(a, itr.next());
            assertEquals(b, itr.next());
            itr.remove();
            try {
                // double remove is illegal.
                itr.remove();
                fail("Expecting: " + IllegalStateException.class);
            } catch (IllegalStateException ex) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);
            }
            assertEquals(c, itr.next());
            assertFalse(itr.hasNext());
        }

        // verify post-remove state.
        assertSameIterator(new String[] { a, c }, buffer.iterator());

    }

    public void test_add_with_null_arg() {
        final RingBuffer<String> buffer = new RingBuffer<String>(3);

        try {
            buffer.add(null);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        assertTrue(buffer.isEmpty());    	
    }
    
    public void test_offer_with_null_arg() {
        final RingBuffer<String> buffer = new RingBuffer<String>(3);

        try {
            buffer.offer(null);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        assertTrue(buffer.isEmpty());    	
    }    

    public void test_offer_full() {
    	final int capacity = 3;
        final RingBuffer<String> buffer = new RingBuffer<String>(capacity);

        //Fill up buffer
        for (int i=0; i < capacity; i++) {
            buffer.offer("a");
        }
        assertTrue(buffer.isFull());
        assertTrue(buffer.size()==capacity);
        
        // Add item to full buffer -- should return false
        if (buffer.offer("d")) {
            fail("Expecting: " + false);
        }
    }   
    
    public void test_addAll_full() {
        final List<String> l = Arrays.asList("a", "b", "c");
        final RingBuffer<String> buffer = new RingBuffer<String>(l.size());
        assertTrue(buffer.addAll(l));
        assertTrue(buffer.isFull());
        assertTrue(buffer.size()==l.size());       
    } 
    
    public void test_addAll_empty() {
    	final int capacity = 3;
        final RingBuffer<String> buffer = new RingBuffer<String>(capacity);
        final List<String> l = Collections.emptyList();
        assertFalse(buffer.addAll(l));
        assertTrue(buffer.isEmpty());
        assertTrue(buffer.size()==l.size());       
    }     
    
    public void test_addAll_overflow() {
        final List<String> l = Arrays.asList("a", "b", "c", "d");
        int capacity = l.size()-1;
        final RingBuffer<String> buffer = new RingBuffer<String>(capacity);
        try {
            buffer.addAll(l);
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        assertTrue(buffer.size()==capacity);
        assertTrue(buffer.isFull());
        
    } 
    
    public void test_clear_full() {
        final List<String> l = Arrays.asList("a", "b", "c");
        final RingBuffer<String> buffer = new RingBuffer<String>(l.size());
        assertTrue(buffer.addAll(l));
        assertTrue(buffer.isFull());
        assertTrue(buffer.size()==l.size());  
        //Clear buffer and assert emptiness
        buffer.clear();
        assertTrue(buffer.isEmpty());                
    } 
    
    public void test_clear_empty() {
        int capacity = 7;    	
        final RingBuffer<String> buffer = new RingBuffer<String>(capacity);
        assertTrue(buffer.isEmpty());
        assertTrue(buffer.size()==0);  
        //Clear buffer and assert emptiness
        buffer.clear();
        assertTrue(buffer.isEmpty());
        assertTrue(buffer.size()==0);  
    }     
    
    public void test_clear_true_full() {
        final List<String> l = Arrays.asList("a", "b", "c");
        final RingBuffer<String> buffer = new RingBuffer<String>(l.size());
        assertTrue(buffer.addAll(l));
        assertTrue(buffer.isFull());
        assertTrue(buffer.size()==l.size());  
        //Clear buffer and assert emptiness
        buffer.clear(true);
        assertTrue(buffer.isEmpty());                
    } 
    
    public void test_clear_true_empty() {
        int capacity = 7;    	
        final RingBuffer<String> buffer = new RingBuffer<String>(capacity);
        assertTrue(buffer.isEmpty());
        assertTrue(buffer.size()==0);  
        //Clear buffer and assert emptiness
        buffer.clear(true);
        assertTrue(buffer.isEmpty());
        assertTrue(buffer.size()==0);  
    }        

    public void test_clear_false_full() {
        final List<String> l = Arrays.asList("a", "b", "c");
        final RingBuffer<String> buffer = new RingBuffer<String>(l.size());
        assertTrue(buffer.addAll(l));
        assertTrue(buffer.isFull());
        assertTrue(buffer.size()==l.size());  
        //Clear buffer and assert emptiness
        buffer.clear(false);
        assertTrue(buffer.isEmpty());                
    } 
    
    public void test_clear_false_empty() {
        int capacity = 7;    	
        final RingBuffer<String> buffer = new RingBuffer<String>(capacity);
        assertTrue(buffer.isEmpty());
        assertTrue(buffer.size()==0);  
        //Clear buffer and assert emptiness
        buffer.clear(false);
        assertTrue(buffer.isEmpty());
        assertTrue(buffer.size()==0);  
    }     
    
    public void test_getHeadIndex_empty() {
        final RingBuffer<String> buffer = new RingBuffer<String>(7);
        assertTrue(buffer.getHeadIndex()==0);    	
    }
    
    public void test_getHeadIndex_overflow() {
    	final int capacity = 7;
        final RingBuffer<String> buffer = new RingBuffer<String>(capacity);
        assertTrue(buffer.getHeadIndex()==0);  
        // Fill to overflow capacity checking head as we go.
        // Remove entries once filled to make room.
        for (int i=0; i < capacity * 3; i++) {
        	if (buffer.isFull()) {
        		buffer.remove();
        	}
        	buffer.add("a");
        	assertTrue(buffer.getHeadIndex()==((i+1)%capacity));
        }        
    }
    
    public void test_getTailIndex_empty() {
            final RingBuffer<String> buffer = new RingBuffer<String>(7);
            assertTrue(buffer.getTailIndex()==0);    	
    }
        
    public void test_getTailIndex_overflow() {
    	final int capacity = 3;
        final RingBuffer<String> buffer = new RingBuffer<String>(capacity);
        assertTrue(buffer.getTailIndex()==0);  
        // Fill to overflow capacity checking tail as we go.
        // Remove entries after filled
        for (int i=0; i < capacity * 3; i++) {
        	if (buffer.isFull()) {
        		buffer.remove();
        	}
            buffer.add("a");
            if (buffer.isFull()) {
            	assertTrue(buffer.getTailIndex()==((i+1)%capacity));
            } else {
            	assertTrue(buffer.getTailIndex()==0);            
            }
        }  
    }
 
    public void test_toArray1_empty() {
    	final int capacity = 3;
        final RingBuffer<String> buffer = new RingBuffer<String>(capacity);
        Object [] emptyArr = buffer.toArray();
        assertTrue(emptyArr.length==0);            
    }
    
    public void test_toArray1_nonempty() {
        Object [] intArr = new Object[] {
        		Integer.valueOf(1),
        		Integer.valueOf(2),
        		Integer.valueOf(3)
        };
        final RingBuffer<Object> buffer = new RingBuffer<Object>(intArr.length);
        buffer.addAll(Arrays.asList(intArr));
        // Checks for same array elements and ordering (LRU to MRU)
        assertSameArray(intArr, buffer.toArray());
    }    
    
    public void test_toArray1_nonempty_oversized() {
        Object [] intArr = new Object[] {
        		Integer.valueOf(1),
        		Integer.valueOf(2),
        		Integer.valueOf(3)
        };
        final RingBuffer<Object> buffer = new RingBuffer<Object>(intArr.length);
        buffer.addAll(Arrays.asList(intArr));
        
        //Create over-sized array to hold return values
        Object[] scratch = new Object[intArr.length + 1];
        //init with non-default values
        for (int i=0; i < intArr.length; i++) { scratch[i] = -1; }
        //Call toArray with over-sized array
        Object[] oversizedArr = 
        	buffer.toArray(scratch);
        // Checks for array identity
        assertTrue(scratch==oversizedArr);
        //Verify that last elem has been nulled
        assertTrue(oversizedArr[oversizedArr.length-1]==null);
        //Verify that ordering was preserved
        for (int i=0; i < intArr.length; i++) {
        	assertTrue(intArr[i]==oversizedArr[i]);
        }
    }       
    
    public void test_remove1_illegal_args() {
    
	    final RingBuffer<String> b = new RingBuffer<String>(1);
	
	    try {
	        b.remove(-1);
	        fail("Expecting: " + IllegalArgumentException.class);
	    } catch (IllegalArgumentException ex) {
	        if (log.isInfoEnabled())
	            log.info("Ignoring excepted exception: " + ex);
	    }
	    try {
	        b.remove(0);
	        fail("Expecting: " + IllegalArgumentException.class);
	    } catch (IllegalArgumentException ex) {
	        if (log.isInfoEnabled())
	            log.info("Ignoring excepted exception: " + ex);
	    }
    }
    
    public void test_remove1_single() {
        String expected = "a";
	    final RingBuffer<String> b = new RingBuffer<String>(1);
	    b.add(expected);
	    String received = b.remove(0);
        assertSame(expected, received);
    }  
    
    // see https://sourceforge.net/apps/trac/bigdata/ticket/101
    public void test_remove_get_order() {
        final String[] expected = new String[] {
        		"a", "b", "c", "d"
        };
	    final RingBuffer<String> b = new RingBuffer<String>(expected.length);
	    //Add entries in order
	    for (int i=0; i < expected.length; i++) {
	    	b.add(expected[i]);
	    	assertSame(expected[i], b.get(i));
	    }
	    assertTrue(expected.length==b.size());
	    
	    //Remove entries in MRU to LRU order -- differs from javadoc order
	    for (int i=(expected.length-1); i >= 0; i--) {
	    	final String getString = b.get(i);
	    	final String removeString = b.remove(i);
	    	assertSame(getString, removeString);	    	
	    }
	    assertTrue(b.isEmpty());
    }
    
    // see https://sourceforge.net/apps/trac/bigdata/ticket/101    
    public void test_remove1_multiple_mru_to_lru() {
        String[] expected = new String[] {
        		"a", "b", "c", "d"
        };
	    final RingBuffer<String> b = new RingBuffer<String>(expected.length);
	    //Add entries in order
	    for (int i=0; i < expected.length; i++) {
	    	b.add(expected[i]);
	    	assertSame(expected[i], b.get(i));
	    }
	    assertTrue(expected.length==b.size());
	    
	    //Remove entries in MRU to LRU order -- tests non-LRU and LRU (on last item) access
	    for (int i=(expected.length-1); i >= 0; i--) {
	    	String s = b.remove(i);
	    	assertSame(expected[i], s);	    	
	    }
	    assertTrue(b.isEmpty());
    }    
    
    // see https://sourceforge.net/apps/trac/bigdata/ticket/101    
    public void test_remove1_multiple_lru_to_mru() {
        String[] expected = new String[] {
        		"a", "b", "c", "d"
        };
	    final RingBuffer<String> b = new RingBuffer<String>(expected.length);
	    //Add entries in order
	    for (int i=0; i < expected.length; i++) {
	    	b.add(expected[i]);
	    	assertSame(expected[i], b.get(i));
	    }
	    assertTrue(expected.length==b.size());
	    
	    //Remove entries in LRU to MRU order
	    for (int i=0; i < expected.length; i++) {
	    	String s = b.remove(0);
	    	assertSame(expected[i], s);	    	
	    }
	    assertTrue(b.isEmpty());
    }        
    
    public void test_element_empty() {
	    final RingBuffer<String> b = new RingBuffer<String>(1);
	    try {
	        b.element();
	        fail("Expecting: " + NoSuchElementException.class);
	    } catch (NoSuchElementException ex) {
	        if (log.isInfoEnabled())
	            log.info("Ignoring excepted exception: " + ex);
	    }
    }     
    
    public void test_element_single() {
	    final RingBuffer<String> b = new RingBuffer<String>(1);
	    String elem = "a";
	    b.add(elem);
	    assertSame(elem, b.element());
    }        
    
    public void test_element_multiple() {
        String[] expected = new String[] {
        		"a", "b", "c", "d"
        };
	    final RingBuffer<String> b = new RingBuffer<String>(expected.length);
	    //Add entries in order
        b.addAll(Arrays.asList(expected));
        
	    //Access entries in order -- removing each LRU along the way
	    for (int i=0; i < expected.length; i++) {
	       assertSame(expected[i], b.element());
	       b.remove();
	    }	    
	    assertTrue(b.isEmpty());	    
    }
    
    public void test_peek_empty() {
	    final RingBuffer<String> b = new RingBuffer<String>(1);
	    assertNull(b.peek());
    }     
    
    public void test_peek_single() {
	    final RingBuffer<String> b = new RingBuffer<String>(1);
	    String elem = "a";
	    b.add(elem);
	    assertSame(elem, b.peek());
    }        
    
    public void test_peek_multiple() {
        String[] expected = new String[] {
        		"a", "b", "c", "d"
        };
	    final RingBuffer<String> b = new RingBuffer<String>(expected.length);
        b.addAll(Arrays.asList(expected));

        //Access entries in order -- removing each LRU along the way
	    for (int i=0; i < expected.length; i++) {
	       assertSame(expected[i], b.peek());
	       b.remove();
	    }	    
	    assertTrue(b.isEmpty());
	    
    }

    public void test_poll_empty() {
	    final RingBuffer<String> b = new RingBuffer<String>(1);
	    assertNull(b.poll());
	    assertTrue(b.isEmpty());	    
    }     

    public void test_poll_single() {
	    final RingBuffer<String> b = new RingBuffer<String>(1);
	    String elem = "a";
	    b.add(elem);
	    assertSame(elem, b.poll());
	    assertTrue(b.isEmpty());	    
    }        

    public void test_poll_multiple() {
        String[] expected = new String[] {
        		"a", "b", "c", "d"
        };
	    final RingBuffer<String> b = new RingBuffer<String>(expected.length);
	    //Add entries in order
        b.addAll(Arrays.asList(expected));

        //Access entries in order
	    for (int i=0; i < expected.length; i++) {
	       assertSame(expected[i], b.poll());
	    }	    
	    assertTrue(b.isEmpty());
    }

    public void test_scanHead_null_ref() {
	    final RingBuffer<String> b = new RingBuffer<String>(1);
        try {
        	b.scanHead(1, null);
	        fail("Expecting: " + IllegalArgumentException.class);
	    } catch (IllegalArgumentException ex) {
	        if (log.isInfoEnabled())
	            log.info("Ignoring excepted exception: " + ex);
	    }        
    }
    
    public void test_scanHead_empty() {
	    final RingBuffer<String> b = new RingBuffer<String>(1);
        assertFalse(b.scanHead(1, "a"));
    }  
    
    public void test_scanHead_single() {
	    String[] elems = {"a"};
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
        assertTrue(b.scanHead(1, "a"));
    }    
    
    public void test_scanHead_multiple_all() {
	    String[] elems = {"a", "b", "c"};
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
        assertTrue(b.scanHead(elems.length, "a"));
    }   
 
    public void test_scanHead_multiple_over_scan() {
	    String[] elems = {"a", "b", "c"};
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
    	//Verify that overscanning works (i.e. nscan > size)
        assertTrue(b.scanHead((elems.length * 2), "a"));
    }   

    public void test_scanHead_multiple_partial_scan() {
	    String[] elems = {"a", "b", "c"};
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
    	// Verify that LRU doesn't appear in partial scans (i.e. nscan < size)
    	for (int i=1; i < elems.length; i++) {
            assertFalse(b.scanHead(i, "a"));    		
    	}
    }   
    
    public void test_scanTail_null_ref() {
	    final RingBuffer<String> b = new RingBuffer<String>(1);
        try {
        	b.scanTail(1, null);
	        fail("Expecting: " + IllegalArgumentException.class);
	    } catch (IllegalArgumentException ex) {
	        if (log.isInfoEnabled())
	            log.info("Ignoring excepted exception: " + ex);
	    }        
    }
    
    public void test_scanTail_nonpositive() {
	    final RingBuffer<String> b = new RingBuffer<String>(1);
        try {
        	b.scanTail(-1, null);
	        fail("Expecting: " + IllegalArgumentException.class);
	    } catch (IllegalArgumentException ex) {
	        if (log.isInfoEnabled())
	            log.info("Ignoring excepted exception: " + ex);
	    }        
        try {
        	b.scanTail(0, null);
	        fail("Expecting: " + IllegalArgumentException.class);
	    } catch (IllegalArgumentException ex) {
	        if (log.isInfoEnabled())
	            log.info("Ignoring excepted exception: " + ex);
	    }        
    }    
    
    public void test_scanTail_empty() {
	    final RingBuffer<String> b = new RingBuffer<String>(1);
        assertFalse(b.scanTail(1, "a"));
    }  
    
    public void test_scanTail_single() {
	    String[] elems = {"a"};
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
        assertTrue(b.scanTail(1, "a"));
    }    
    
    public void test_scanTail_multiple_all() {
	    String[] elems = {"a", "b", "c"};
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
        assertTrue(b.scanTail(elems.length, "c"));
    }   
    
    public void test_scanTail_multiple_over_scan() {
	    String[] elems = {"a", "b", "c"};
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
    	//Verify that overscanning works (i.e. nscan > size)
        assertTrue(b.scanTail((elems.length * 2), "c"));
    }   

    public void test_scanTail_multiple_partial_scan() {
	    String[] elems = {"a", "b", "c"};
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
    	// Verify that MRU doesn't appear in partial scans (i.e. nscan < size)
    	for (int i=1; i < elems.length; i++) {
            assertFalse(b.scanTail(i, "c"));    		
    	}
    }  
    
    public void test_contains_empty() {
    	final RingBuffer<String> b = new RingBuffer<String>(1);	
   	    assertFalse(b.contains("a"));
    }
    
    public void test_contains_missing() {
	    String[] elems = {"a", "b", "c"};
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
   	    assertFalse(b.contains("d"));
    }

    public void test_contains_each() {
	    String[] elems = {"a", "b", "c"};
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
   	    assertTrue(b.contains("a"));
   	    assertTrue(b.contains("b"));
   	    assertTrue(b.contains("c"));
    }
    
    public void test_contains_null() {
        final RingBuffer<String> b = new RingBuffer<String>(1);
        try {
            b.contains(null);
            fail("Expecting: " + NullPointerException.class);
        } catch (NullPointerException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring excepted exception: " + ex);
        }
    }
    
    public void test_contains_all_null() {
    	final RingBuffer<String> b = new RingBuffer<String>(1);	
        try {
            b.containsAll(null);
            fail("Expecting: " + NullPointerException.class);
        } catch (NullPointerException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring excepted exception: " + ex);
        }
    }
    
    public void test_contains_all_this() {
    	final RingBuffer<String> b = new RingBuffer<String>(1);
    	// Note: This is a tautology.
    	assertTrue(b.containsAll(b));
//        try {
//            b.containsAll(b);
//            fail("Expecting: " + IllegalArgumentException.class);
//        } catch (IllegalArgumentException ex) {
//            if (log.isInfoEnabled())
//                log.info("Ignoring excepted exception: " + ex);
//        }
    }

    public void test_contains_all_empty() {
    	final RingBuffer<String> b = new RingBuffer<String>(1);	
   	    assertFalse(b.contains(Arrays.asList("a")));
    }
    
    public void test_contains_all_missing() {
	    String[] elems = {"a", "b", "c"};
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
   	    assertFalse(b.containsAll(Arrays.asList("d")));
    }

    public void test_contains_all_subset() {
	    String[] elems = {"a", "b", "c"};
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
    	for (int i=1; i < elems.length; i++) {
    		String[] partial = Arrays.copyOf(elems, i);
    		assertTrue(b.containsAll(Arrays.asList(partial)));
    	}
    }
    
    public void test_contains_all_superset() {
	    String[] superset = {"a", "b", "c"};
	    String[] subset = Arrays.copyOf(superset, superset.length-1);	    
    	final RingBuffer<String> b = new RingBuffer<String>(subset.length);	
    	b.addAll(Arrays.asList(subset));
        assertFalse(b.containsAll(Arrays.asList(superset)));
    }    
    
    public void test_iterator_hasnext_empty() {
    	final RingBuffer<String> b = new RingBuffer<String>(1);	
    	assertTrue(b.size()==0);
    	Iterator<String> iter = b.iterator();
    	assertFalse(iter.hasNext());
    	try {
    		iter.next();
            fail("Expecting: " + NoSuchElementException.class);
    	} catch (NoSuchElementException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring excepted exception: " + ex);   		
    	}
    }
    
    public void test_iterator_hasnext_single() {
	    String[] elems = {"a"};
	    test_iterator_hasnext_common(elems);
    }

    public void test_iterator_hasnext_multiple() {
	    String[] elems = {"a", "b", "c", "d"};
	    test_iterator_hasnext_common(elems);
    }
    
    private void test_iterator_hasnext_common(String[] elems) {
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
		Iterator<String> iter = b.iterator();
    	for (int i=0; i < elems.length; i++) {
    		assertTrue(iter.hasNext());
    		String n = iter.next();
    		assertEquals(elems[i], n);
    	}   	
    	assertFalse(iter.hasNext());
    }  
    
    public void test_iterator_hasnext_remove_empty() {
    	final RingBuffer<String> b = new RingBuffer<String>(1);	
    	try {
    		b.iterator().remove();
            fail("Expecting: " + IllegalStateException.class);
   	    } catch (IllegalStateException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring excepted exception: " + ex);   		   		
    	}    	
    }
    
    public void test_iterator_hasnext_remove_single() {
	    String[] elems = {"a"};
	    test_iterator_hasnext_remove_common(elems);
    }

    public void test_iterator_hasnext_remove_multiple() {
	    String[] elems = {"a", "b", "c", "d"};
	    test_iterator_hasnext_remove_common(elems);
    }
    
    private void test_iterator_hasnext_remove_common(String[] elems) {
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
		Iterator<String> iter = b.iterator();
    	for (int i=0; i < elems.length; i++) {
    		assertTrue(iter.hasNext());
    		String n = iter.next();
    		assertEquals(elems[i], n);
    		iter.remove();
    	}   	
    	assertFalse(iter.hasNext());
    	assertTrue(b.isEmpty());
    }    
    
    public void test_iterator_remove_ref_single() {
	    String[] elems = {"a"};
	    test_remove_ref_common(elems);
    }
    
    public void test_iterator_remove_ref_multiple() {
	    String[] elems = {"a", "b", "c", "d"};
	    test_remove_ref_common(elems);
    }

    private void test_remove_ref_common(String[] elems) {
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
    	for (int i=0; i < elems.length; i++) {
    		b.remove(elems[i]);
    	}   	
    	assertTrue(b.isEmpty());
    }    

    public void test_iterator_remove_ref_missing() {
	    String[] elems = {"a", "b", "c", "d"};
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
    	assertFalse(b.remove("z"));
    }
    

    public void test_iterator_remove_all_null_existing() {
	    String[] elems = {"a", "b", "c", "d"};
     	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	 
    	b.addAll(Arrays.asList(elems));     	
    	try {
    		b.removeAll(null);
            fail("Expecting: " + NullPointerException.class);    		
    	} catch (NullPointerException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring excepted exception: " + ex);   		   		    		
    	}
    }
    
    public void test_iterator_remove_all_missing() {
	    String[] elems = {"a", "b", "c", "d"};
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
    	assertFalse(b.removeAll(Arrays.asList("z")));
    }  
    
    public void test_iterator_remove_all() {
	    String[] elems = {"a", "b", "c", "d"};
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
    	assertTrue(b.removeAll(Arrays.asList(elems)));
    	assertTrue(b.isEmpty());
    }    
    
    public void test_iterator_remove_all_subset() {
	    String[] elems = {"a", "b", "c", "d"};
	    String[] subset = Arrays.copyOf(elems, elems.length-1);
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
    	assertTrue(b.removeAll(Arrays.asList(subset)));
    	assertFalse(b.isEmpty());
    	assertTrue(b.size()==1);
    }    
    
    public void test_iterator_remove_all_superset() {
	    String[] superset = {"a", "b", "c", "d"};
	    String[] elems = Arrays.copyOf(superset, superset.length-1);
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(Arrays.asList(elems));
    	assertTrue(b.removeAll(Arrays.asList(superset)));
    	assertTrue(b.isEmpty());
    }      
    
    public void test_iterator_retain_all() {
	    String[] elems = {"a", "b", "c", "d"};
	    List<String> l = Arrays.asList(elems);
    	final RingBuffer<String> b = new RingBuffer<String>(elems.length);	
    	b.addAll(l);
    	assertFalse(b.retainAll(l));
    }  

    public void test_iterator_retain_all_subset() {
	    String[] superset = {"a", "b", "c", "d"};
	    String[] subset = Arrays.copyOf(superset, superset.length-1);
    	final RingBuffer<String> b = new RingBuffer<String>(superset.length);	
    	b.addAll(Arrays.asList(superset));
    	assertTrue(b.retainAll(Arrays.asList(subset)));
    	assertTrue(b.size()==subset.length);
    	assertSameArray(subset, b.toArray());
    	
    }     
}
