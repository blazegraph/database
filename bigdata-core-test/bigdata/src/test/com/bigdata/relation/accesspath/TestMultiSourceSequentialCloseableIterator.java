/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase2;

/**
 * Test suite for the {@link MultiSourceSequentialCloseableIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMultiSourceSequentialCloseableIterator extends TestCase2 {
    
    public TestMultiSourceSequentialCloseableIterator() {
        
    }

    public TestMultiSourceSequentialCloseableIterator(String name) {
        super(name);
    }

    private final ThickAsynchronousIterator<String> emptyIterator() {
        return new ThickAsynchronousIterator<String>(new String[]{});
    }
    
    private final ThickAsynchronousIterator<String> iterator(final String... a) {
        return new ThickAsynchronousIterator<String>(a);
    }
    
    public void test1() throws InterruptedException {
        
        // empty iterator.
        final MultiSourceSequentialCloseableIterator<String> itr = new MultiSourceSequentialCloseableIterator<String>(
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
        assertTrue(itr.hasNext());
        assertEquals("a", itr.next());
//        assertTrue(itr.hasNext(1, TimeUnit.MILLISECONDS));
//        assertEquals("a", itr.next(1, TimeUnit.MILLISECONDS));

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
//        assertFalse(itr.hasNext(1, TimeUnit.MILLISECONDS));
//        assertNull(itr.next(1, TimeUnit.MILLISECONDS));

        // can not add more sources.
        assertFalse(itr.add(new ThickAsynchronousIterator<String>(
                new String[] { "b" })));

    }

    public void test2() throws InterruptedException {

        // empty iterator.
        final MultiSourceSequentialCloseableIterator<String> itr = new MultiSourceSequentialCloseableIterator<String>(
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
     * @throws ExecutionException 
     */
    public void test3() throws InterruptedException, ExecutionException {

        // empty iterator.
        final MultiSourceSequentialCloseableIterator<String> itr = new MultiSourceSequentialCloseableIterator<String>(
                emptyIterator());

        final ExecutorService service = Executors.newSingleThreadExecutor();

        try {

            final FutureTask<Void> ft = new FutureTask<Void>(new Callable<Void>() {

                public Void call() throws Exception {

                    log.info("Will wait on iterator.");

                    assertFalse(itr.hasNext());
//                    if (itr.hasNext(1000, TimeUnit.MILLISECONDS))
//                        fail("Iterator should not visit anything.");

                    // Can not add more sources.
                    assertFalse(itr.add(new ThickAsynchronousIterator<String>(
                            new String[] { "b" })));
                    
                    return null;
                    
                }

            });

            service.submit(ft);
            
            Thread.sleep(500/*ms*/);
            
            log.info("Will close iterator.");
            itr.close();

            // check future.
            ft.get();

        } finally {
            
            service.shutdownNow();

        }

    }
    
    /**
     * Verify that the iterator closes all sources iterators when it is closed.
     * 
     * @throws InterruptedException
     */
    public void test4_sources_closed() throws InterruptedException {

        final ThickAsynchronousIterator<String> itr1 = iterator("a","b","c");
        
        // empty iterator.
        final MultiSourceSequentialCloseableIterator<String> itr = new MultiSourceSequentialCloseableIterator<String>(
                itr1);

        assertEquals("a", itr.next());
//        assertEquals("b", itr.next());
        
        // more is available from the high level iterator.
        assertTrue(itr.hasNext());

        // more is available from the underlying iterator.
        assertTrue(itr1.hasNext());

        log.info("Will close iterator.");
        itr.close();

        // can not add more sources.
        assertFalse(itr.add(iterator("d")));

        // underlying iterator was closed.
        assertFalse(itr1.open);
        assertFalse(itr1.hasNext());
        
        // high level iterator was closed.
        assertFalse(itr.hasNext());
        
    }

    /**
     * Verify that sources are closed when there is more than one source.
     * 
     * @throws InterruptedException
     */
    public void test5_sources_closed() throws InterruptedException {

        final ThickAsynchronousIterator<String> itr1 = iterator("a","b","c");
        final ThickAsynchronousIterator<String> itr2 = iterator("d","e","f");
        final ThickAsynchronousIterator<String> itr3 = iterator("g","h","i");
        
        // empty iterator.
        final MultiSourceSequentialCloseableIterator<String> itr = new MultiSourceSequentialCloseableIterator<String>(
                itr1);
        itr.add(itr2);
        itr.add(itr3);

        assertEquals("a", itr.next());
        assertEquals("b", itr.next());
        assertEquals("c", itr.next());
        
        // more is available from the high level iterator.
        assertTrue(itr.hasNext());

        // 1st underlying iterator was closed.
        assertFalse(itr1.hasNext());

        log.info("Will close iterator.");
        itr.close();

        // can not add more sources.
        assertFalse(itr.add(iterator("xxx")));

        // remaining underlying iterators were closed.
        assertFalse(itr1.open);
        assertFalse(itr1.hasNext());
        assertFalse(itr2.open);
        assertFalse(itr2.hasNext());
        assertFalse(itr3.open);
        assertFalse(itr3.hasNext());
        
        // high level iterator was closed.
        assertFalse(itr.hasNext());
        
    }

    private static class ThickAsynchronousIterator<E> implements
            IAsynchronousIterator<E>, Serializable {

        private static final long serialVersionUID = 1L;

        private transient boolean open = true;

        /**
         * Index of the last element visited by {@link #next()} and
         * <code>-1</code> if NO elements have been visited.
         */
        private int lastIndex;

        /**
         * The array of elements to be visited by the iterator.
         */
        private final E[] a;

        /**
         * Create a thick iterator.
         * 
         * @param a
         *            The array of elements to be visited by the iterator (may
         *            be empty, but may not be <code>null</code>).
         * 
         * @throws IllegalArgumentException
         *             if <i>a</i> is <code>null</code>.
         */
        public ThickAsynchronousIterator(final E[] a) {

            if (a == null)
                throw new IllegalArgumentException();

            this.a = a;

            lastIndex = -1;

        }

        public boolean hasNext() {

            if(open && lastIndex + 1 < a.length)
                return true;
            
            close();
           
            return false;

        }

        public E next() {

            if (!hasNext())
                throw new NoSuchElementException();

            return a[++lastIndex];

        }

        public void remove() {

            throw new UnsupportedOperationException();

        }

        /*
         * ICloseableIterator.
         */

        public void close() {

            open = false;

        }

        /*
         * IAsynchronousIterator.
         */

        public boolean isExhausted() {

            return !hasNext();

        }

        /**
         * Delegates to {@link #hasNext()} since all data are local and timeouts
         * can not occur.
         */
        public boolean hasNext(long timeout, TimeUnit unit) {

            return hasNext();

        }

        /**
         * Delegates to {@link #next()} since all data are local and timeouts
         * can not occur.
         */
        public E next(long timeout, TimeUnit unit) {

            return next();

        }

    }

}
