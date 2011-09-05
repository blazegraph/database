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
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase2;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariableOrConstant;
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

    /**
     * Throw an exception for the first operator having a ground difference
     * (different Class, different arity, or different annotation). When both
     * operators have the same named annotation but the annotation values differ
     * and they are both bop valued annotations, then the difference will be
     * reported for the annotation bops.
     * 
     * @param sb
     * @param o1
     * @param o2
     */
    public static void diff(final BOp o1, final BOp o2) {

        if (log.isDebugEnabled())
            log.debug("Comparing: "
                + (o1 == null ? "null" : o1.toShortString()) + " with "
                + (o2 == null ? "null" : o2.toShortString()));
        
        if (o1 == o2) // same ref, including null.
            return;

        if (o1 == null && o2 != null) {

            fail("Expecting null, but have " + o2);

        }
        
        if (!o1.getClass().equals(o2.getClass())) {

            fail("Types differ: expecting " + o1.getClass() + ", but have "
                    + o2.getClass() + " for " + o1.toShortString() + ", "
                    + o2.toShortString() + "\n");

        }

        final int arity1 = o1.arity();

        final int arity2 = o2.arity();
        
        if (arity1 != arity2) {
         
            fail("Arity differs: expecting " + arity1 + ", but have " + arity2
                    + " for " + o1.toShortString() + ", " + o2.toShortString()
                    + "\n");
            
        }

        for (int i = 0; i < arity1; i++) {

            final BOp c1 = o1.get(i);
            final BOp c2 = o2.get(i);

            diff(c1, c2);

        }

        for (String name : o1.annotations().keySet()) {

            final Object a1 = o1.getProperty(name);

            final Object a2 = o2.getProperty(name);

            if (log.isDebugEnabled())
                log.debug("Comparing: "
                    + o1.getClass().getSimpleName()
                    + " @ \""
                    + name
                    + "\" having "
                    + (a1 == null ? "null" : (a1 instanceof BOp ? ((BOp) a1)
                            .toShortString() : a1.toString()))//
                    + " with "
                    + //
                    (a2 == null ? "null" : (a2 instanceof BOp ? ((BOp) a2)
                            .toShortString() : a2.toString()))//
            );

            if (a1 == a2) // same ref, including null.
                continue;

            if (a1 == null && a2 != null) {
                
                fail("Not expecting annotation for " + name + " : expecting="
                        + o1 + ", actual=" + o2);

            }

            if (a2 == null) {

                fail("Missing annotation @ \"" + name + "\" : expecting=" + o1
                        + ", actual=" + o2);

            }

            if (a1 instanceof BOp && a2 instanceof BOp) {

                // Compare BOPs in depth.
                diff((BOp) a1, (BOp) a2);

            } else {

                if (!a1.equals(a2)) {

                    fail("Annotations differ for " + name + "  : expecting="
                            + o1 + ", actual=" + o2);

                }

            }

        }
        
        final int n1 = o1.annotations().size();
        
        final int n2 = o2.annotations().size();

        if (n1 != n2) {

            if (n1 > n2) {

                final Set<String> expectedSet = new LinkedHashSet<String>(o1
                        .annotations().keySet());

                final Set<String> actualSet = new LinkedHashSet<String>(o2
                        .annotations().keySet());
                
                expectedSet.removeAll(actualSet);
                
                fail("#of annotations differs: expecting " + o1 + ", actual="
                        + o2 + ", missing=" + expectedSet.toString());
                
            } else {
                
                final Set<String> expectedSet = new LinkedHashSet<String>(o1
                        .annotations().keySet());

                final Set<String> actualSet = new LinkedHashSet<String>(o2
                        .annotations().keySet());
                
                actualSet.removeAll(expectedSet);
                
                fail("#of annotations differs: expecting " + o1 + ", actual="
                        + o2 + ", missing=" + actualSet.toString());
                
            }
            
            fail("#of annotations differs: expecting " + o1 + ", actual=" + o2);

        }

        if(o1 instanceof IVariableOrConstant<?>) {
            
            /*
             * Note: Var and Constant both have a piece of non-BOp data which is
             * their name (Var) and their Value (Constant). The only way to
             * check those things is by casting or using Var.equals() or
             * Constant.equals().
             */

            if (!o1.equals(o2)) {

                // A difference in the non-BOp value of the variable or
                // constant.

                fail("Expecting: " + o1 + ", actual=" + o2);
                
            }
            
        }
        
        if (!o1.equals(o2)) {

//            o1.equals(o2); // debug point.
            
            fail("Failed to detect difference reported by equals(): expected="
                    + o1 + ", actual=" + o2);
            
        }
        
    }

}
