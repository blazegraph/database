/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Oct 6, 2010
 */

package com.bigdata.relation.accesspath;

import java.util.concurrent.TimeUnit;

import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

import junit.framework.TestCase2;

/**
 * Test suite for the {@link MultiSourceSequentialAsynchronousIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMultiSourceSequentialAsynchronousIterator extends TestCase2 {
    
    public TestMultiSourceSequentialAsynchronousIterator() {
        
    }

    public TestMultiSourceSequentialAsynchronousIterator(String name) {
        super(name);
    }

    private final IAsynchronousIterator<String> emptyIterator() {
        return new ThickAsynchronousIterator<String>(new String[]{});
    }
    
    public void test1() throws InterruptedException {
        
        // empty iterator.
        final MultiSourceSequentialAsynchronousIterator<String> itr = new MultiSourceSequentialAsynchronousIterator<String>(
                emptyIterator());
        
//        // nothing available yet.
//        assertFalse(itr.hasNext(1, TimeUnit.MILLISECONDS));
//        assertNull(itr.next(1, TimeUnit.MILLISECONDS));

        // add an empty chunk.
        assertTrue(itr.add(new ThickAsynchronousIterator<String>(
                new String[] {})));

//        // still nothing available yet.
//        assertFalse(itr.hasNext(1, TimeUnit.MILLISECONDS));
//        assertNull(itr.next(1, TimeUnit.MILLISECONDS));

        // add a non-empty chunk.
        assertTrue(itr.add(new ThickAsynchronousIterator<String>(
                new String[] { "a" })));

        // reports data available and visits data.
        assertTrue(itr.hasNext(1, TimeUnit.MILLISECONDS));
        assertEquals("a", itr.next(1, TimeUnit.MILLISECONDS));

        // add a non-empty chunk.
        assertTrue(itr.add(new ThickAsynchronousIterator<String>(
                new String[] { "b" })));

        // reports data available and visits data.
        assertTrue(itr.hasNext());
        assertEquals("b", itr.next());

        // close the iterator.
        itr.close();

        // iterator reports nothing available.
        assertFalse(itr.hasNext());
        assertFalse(itr.hasNext(1, TimeUnit.MILLISECONDS));
        assertNull(itr.next(1, TimeUnit.MILLISECONDS));

        // can not add more sources.
        assertFalse(itr.add(new ThickAsynchronousIterator<String>(
                new String[] { "b" })));

    }

    public void test2() throws InterruptedException {

        // empty iterator.
        final MultiSourceSequentialAsynchronousIterator<String> itr = new MultiSourceSequentialAsynchronousIterator<String>(
                emptyIterator());

        // add a non-empty chunk.
        assertTrue(itr.add(new ThickAsynchronousIterator<String>(
                new String[] { "a" })));

        // add a non-empty chunk.
        assertTrue(itr.add(new ThickAsynchronousIterator<String>(
                new String[] { "b" })));
        
        // reports data available and visits data.
        assertTrue(itr.hasNext());
        assertEquals("a", itr.next());
        assertTrue(itr.hasNext());
        assertEquals("b", itr.next());

        // another read on the iterator causes it to be closed.
        assertFalse(itr.hasNext());

        // can not add more sources.
        assertFalse(itr.add(new ThickAsynchronousIterator<String>(
                new String[] { "b" })));

    }

    /**
     * Verify that the iterator notices if it is asynchronously closed.
     * 
     * @throws InterruptedException
     */
    public void test3() throws InterruptedException {

        // empty iterator.
        final MultiSourceSequentialAsynchronousIterator<String> itr = new MultiSourceSequentialAsynchronousIterator<String>(
                emptyIterator());

        new Thread() {

            public void run() {
                try {
                    log.info("Will wait on iterator.");
                    if (itr.hasNext(2000, TimeUnit.MILLISECONDS))
                        fail("Iterator should not visit anything.");
                } catch (Throwable t) {
                    log.error(t, t);
                }
            }
            
        }.start();

        log.info("Sleeping...");
        Thread.sleep(500/*milliseconds.*/);

        log.info("Will close iterator.");
        itr.close();

        // can not add more sources.
        assertFalse(itr.add(new ThickAsynchronousIterator<String>(
                new String[] { "b" })));

    }
    
}
