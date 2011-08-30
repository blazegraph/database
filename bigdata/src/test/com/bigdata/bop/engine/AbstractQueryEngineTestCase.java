/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Aug 30, 2011
 */

package com.bigdata.bop.engine;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import junit.framework.TestCase2;

import com.bigdata.bop.IBindingSet;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Base class adds some static methods used with unit tests of the
 * {@link QueryEngine}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractQueryEngineTestCase extends TestCase2 {

    protected static final Logger log = Logger
            .getLogger(AbstractQueryEngineTestCase.class);
    
    public AbstractQueryEngineTestCase() {
        super();
    }
    
    public AbstractQueryEngineTestCase(final String name) {
        super(name);
    }
    
    /**
     * Verify the expected solutions.
     * 
     * @param expected
     * @param itr
     * 
     * @deprecated by {@link #assertSameSolutions(Future, IBindingSet[], IAsynchronousIterator)}
     */
    static public void assertSameSolutions(final IBindingSet[] expected,
            final IAsynchronousIterator<IBindingSet[]> itr) {
        
        try {
            int n = 0;
            while (itr.hasNext()) {
                final IBindingSet[] e = itr.next();
                if (log.isInfoEnabled())
                    log.info(n + " : chunkSize=" + e.length);
                for (int i = 0; i < e.length; i++) {
                    if (log.isInfoEnabled())
                        log.info(n + " : " + e[i]);
                    if (n >= expected.length) {
                        fail("Willing to deliver too many solutions: n=" + n
                                + " : " + e[i]);
                    }
                    if (!expected[n].equals(e[i])) {
                        fail("n=" + n + ", expected=" + expected[n]
                                + ", actual=" + e[i]);
                    }
                    n++;
                }
            }
            assertEquals("Wrong number of solutions", expected.length, n);
        } finally {
            itr.close();
        }
    
    }

    /**
     * Verify the expected solutions.
     * 
     * @param expected
     *            The expected solutions.
     * @param runningQuery
     *            The running query whose solutions will be verified.
     */
    static public void assertSameSolutions(
            final IBindingSet[] expected,
            final IRunningQuery runningQuery) {
        assertSameSolutions(expected, runningQuery.iterator(), runningQuery);
    }

    /**
     * Verify the expected solutions.
     * 
     * @param expected
     *            The expected solutions.
     * @param itr
     *            The iterator draining the query.
     * @param ft
     *            The future of the query.
     */
    static public void assertSameSolutions(
                final IBindingSet[] expected,
                final IAsynchronousIterator<IBindingSet[]> itr,
                final Future<Void> ft
            ) {
        try {
            int n = 0;
            if(ft!=null&&ft.isDone()) ft.get();
            while (itr.hasNext()) {
                if(ft!=null&&ft.isDone()) ft.get();
                final IBindingSet[] e = itr.next();
                if(ft!=null&&ft.isDone()) ft.get();
                if (log.isInfoEnabled())
                    log.info(n + " : chunkSize=" + e.length);
                for (int i = 0; i < e.length; i++) {
                    if (log.isInfoEnabled())
                        log.info(n + " : " + e[i]);
                    if (n >= expected.length) {
                        fail("Willing to deliver too many solutions: n=" + n
                                + " : " + e[i]);
                    }
                    if (!expected[n].equals(e[i])) {
                        fail("n=" + n + ", expected=" + expected[n]
                                + ", actual=" + e[i]);
                    }
                    n++;
                }
            }
            if(ft!=null) ft.get();
            assertEquals("Wrong number of solutions", expected.length, n);
        } catch (InterruptedException ex) {
            throw new RuntimeException("Query evaluation was interrupted: "
                    + ex, ex);
        } catch(ExecutionException ex) {
            throw new RuntimeException("Error during query evaluation: " + ex,
                    ex);
        } finally {
            itr.close();
        }
    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     * <p>
     * Note: If the objects being visited do not correctly implement hashCode()
     * and equals() then this can fail even if the desired objects would be
     * visited. When this happens, fix the implementation classes.
     * 
     * @deprecated by the version which passes the {@link IRunningQuery}
     */
    static public <T> void assertSameSolutionsAnyOrder(final T[] expected,
            final Iterator<T> actual) {
    
        assertSameSolutionsAnyOrder("", expected, actual);
    
    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     * <p>
     * Note: If the objects being visited do not correctly implement hashCode()
     * and equals() then this can fail even if the desired objects would be
     * visited. When this happens, fix the implementation classes.
     * 
     * @deprecated by the version which passes the {@link IRunningQuery}
     */
    static public <T> void assertSameSolutionsAnyOrder(final String msg,
            final T[] expected, final Iterator<T> actual) {
    
        try {
    
            /*
             * Populate a map that we will use to realize the match and
             * selection without replacement logic. The map uses counters to
             * handle duplicate keys. This makes it possible to write tests in
             * which two or more binding sets which are "equal" appear.
             */
    
            final int nrange = expected.length;
    
            final java.util.Map<T, AtomicInteger> range = new java.util.LinkedHashMap<T, AtomicInteger>();
    
            for (int j = 0; j < nrange; j++) {
    
                AtomicInteger count = range.get(expected[j]);
    
                if (count == null) {
    
                    count = new AtomicInteger();
    
                }
    
                range.put(expected[j], count);
    
                count.incrementAndGet();
                
            }
    
            // Do selection without replacement for the objects visited by
            // iterator.
    
            for (int j = 0; j < nrange; j++) {
    
                if (!actual.hasNext()) {
    
                    fail(msg
                            + ": Iterator exhausted while expecting more object(s)"
                            + ": index=" + j);
    
                }
    
                final T actualObject = actual.next();
    
                if (log.isInfoEnabled())
                    log.info("visting: " + actualObject);
    
                AtomicInteger counter = range.get(actualObject);
    
                if (counter == null || counter.get() == 0) {
    
                    fail("Object not expected" + ": index=" + j + ", object="
                            + actualObject);
    
                }
    
                counter.decrementAndGet();
                
            }
    
            if (actual.hasNext()) {
    
                fail("Iterator will deliver too many objects.");
    
            }
    
        } finally {
    
            if (actual instanceof ICloseableIterator<?>) {
    
                ((ICloseableIterator<T>) actual).close();
    
            }
    
        }
    
    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     * <p>
     * Note: If the objects being visited do not correctly implement hashCode()
     * and equals() then this can fail even if the desired objects would be
     * visited. When this happens, fix the implementation classes.
     */
    static public void assertSameSolutionsAnyOrder(final IBindingSet[] expected,
            final IRunningQuery runningQuery) {
    
        assertSameSolutionsAnyOrder("", expected, runningQuery);
    
    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     * <p>
     * Note: If the objects being visited do not correctly implement hashCode()
     * and equals() then this can fail even if the desired objects would be
     * visited. When this happens, fix the implementation classes.
     */
    static public void assertSameSolutionsAnyOrder(final String msg,
            final IBindingSet[] expected, final IRunningQuery runningQuery) {
    
    	final IAsynchronousIterator<IBindingSet[]> itr = runningQuery
    			.iterator();
    
    	assertSameSolutionsAnyOrder(msg, expected, itr, runningQuery/* future */);
        
    }

    static public void assertSameSolutionsAnyOrder(
            final IBindingSet[] expected,
            final IAsynchronousIterator<IBindingSet[]> itr,
            final Future<?> future) {
    
        assertSameSolutionsAnyOrder("", expected, itr, future);
        
    }

    static public void assertSameSolutionsAnyOrder(final String msg,
    		final IBindingSet[] expected,
    		final IAsynchronousIterator<IBindingSet[]> itr,
    		final Future<?> runningQuery) {
    
        try {
    
            final Iterator<IBindingSet> actual = new Dechunkerator<IBindingSet>(
                    itr);
    
            /*
             * Populate a map that we will use to realize the match and
             * selection without replacement logic. The map uses counters to
             * handle duplicate keys. This makes it possible to write tests in
             * which two or more binding sets which are "equal" appear.
             */
    
            final int nrange = expected.length;
    
            final java.util.Map<IBindingSet, AtomicInteger> range = new java.util.LinkedHashMap<IBindingSet, AtomicInteger>();
    
            for (int j = 0; j < nrange; j++) {
    
                AtomicInteger count = range.get(expected[j]);
    
                if (count == null) {
    
                    count = new AtomicInteger();
    
                }
    
                range.put(expected[j], count);
    
                count.incrementAndGet();
                
            }
    
            // Do selection without replacement for the objects visited by
            // iterator.
    
            for (int j = 0; j < nrange; j++) {
    
                if (!actual.hasNext()) {
    
                    if(runningQuery.isDone()) runningQuery.get();
                    
                    fail(msg
                            + ": Iterator exhausted while expecting more object(s)"
                            + ": index=" + j);
    
                }
    
                if(runningQuery.isDone()) runningQuery.get();
    
                final IBindingSet actualObject = actual.next();
    
                if(runningQuery.isDone()) runningQuery.get();
    
                if (log.isInfoEnabled())
                    log.info("visting: " + actualObject);
    
                final AtomicInteger counter = range.get(actualObject);
    
                if (counter == null || counter.get() == 0) {
    
                    fail("Object not expected" + ": index=" + j + ", object="
                            + actualObject);
    
                }
    
                counter.decrementAndGet();
                
            }
    
            if (actual.hasNext()) {
    
                fail("Iterator will deliver too many objects.");
    
            }
            
            // The query should be done. Check its Future.
            runningQuery.get();
    
        } catch (InterruptedException ex) {
            
            throw new RuntimeException("Query evaluation was interrupted: "
                    + ex, ex);
            
        } catch(ExecutionException ex) {
        
            throw new RuntimeException("Error during query evaluation: " + ex,
                    ex);
    
        } finally {
    
            itr.close();
            
        }
    
    }

    /**
     * Return an {@link IAsynchronousIterator} that will read a single,
     * empty {@link IBindingSet}.
     * 
     * @param bindingSet
     *            the binding set.
     */
    static public ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet bindingSet) {
    
        return new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[] { bindingSet } });
    
    }

    /**
     * Return an {@link IAsynchronousIterator} that will read a single, chunk
     * containing all of the specified {@link IBindingSet}s.
     * 
     * @param bindingSets
     *            the binding sets.
     */
    static public ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet[] bindingSets) {
    
        return new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { bindingSets });
    
    }

    /**
     * Return an {@link IAsynchronousIterator} that will read a single, chunk
     * containing all of the specified {@link IBindingSet}s.
     * 
     * @param bindingSetChunks
     *            the chunks of binding sets.
     */
    static public ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet[][] bindingSetChunks) {
    
        return new ThickAsynchronousIterator<IBindingSet[]>(bindingSetChunks);
    
    }

}
