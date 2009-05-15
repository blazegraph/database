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

import java.util.Iterator;
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
            new RingBuffer(0);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring excepted exception: " + ex);
        }
        
        try {
            new RingBuffer(-1);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring excepted exception: " + ex);
        }

        final RingBuffer b = new RingBuffer(1);

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
     * @todo must also test when remove not at the tail!
     * 
     * When removing the tail, head := (head-1) % capacity.
     */
    public void test_removeNth() {

        final String a = "a";
        final String b = "b";
        final String c = "c";
        final String d = "d";
        
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

}
