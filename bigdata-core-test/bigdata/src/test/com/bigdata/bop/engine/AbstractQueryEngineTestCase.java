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
Portions of this code are:

Copyright Aduna (http://www.aduna-software.com/) � 2001-2007

All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    * Neither the name of the copyright holder nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/
/*
 * Created on Aug 30, 2011
 */

package com.bigdata.bop.engine;

import info.aduna.iteration.Iterations;
import info.aduna.text.StringUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.model.util.ModelUtil;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResultUtil;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.MutableTupleQueryResult;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.striterator.Dechunkerator;

import cutthecrap.utils.striterators.ICloseableIterator;

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
                final ICloseableIterator<IBindingSet[]> itr,
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
    
                final List<T> remaining = new LinkedList<T>();
                
                while(actual.hasNext())
                    remaining.add(actual.next());
                
                fail("Iterator will deliver too many objects: remaining="
                        + remaining);

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
    
    	final ICloseableIterator<IBindingSet[]> itr = runningQuery
    			.iterator();
    
    	assertSameSolutionsAnyOrder(msg, expected, itr, runningQuery/* future */);
        
    }

    static public void assertSameSolutionsAnyOrder(
            final IBindingSet[] expected,
            final ICloseableIterator<IBindingSet[]> itr,
            final Future<?> future) {
    
        assertSameSolutionsAnyOrder("", expected, itr, future);
        
    }

    static public void assertSameSolutionsAnyOrder(final String msg,
    		final IBindingSet[] expected,
    		final ICloseableIterator<IBindingSet[]> itr,
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

        if (o1 != null && o2 == null) {

            fail("Expecting non-null, but have null: expected=" + o1);

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

                final boolean tst;
                if(a1.getClass().isArray()) {
                    tst = Arrays.equals((Object[])a1, (Object[])a2);
                } else {
                    tst = a1.equals(a2);
                }
                if(!tst) {
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
                
                fail("#of annotations differs: expecting=" + o1 + "\nactual="
                        + o2 + "\nmissing=\n" + actualSet.toString());
                
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

//                if (o1 instanceof Constant && o2 instanceof Constant) {
//                    
//                    @SuppressWarnings({ "rawtypes", "unchecked" })
//                    final Constant<IV> c1 = (Constant<IV>) o1;
//
//                    @SuppressWarnings({ "rawtypes", "unchecked" })
//                    final Constant<IV> c2 = (Constant<IV>) o2;
//
//                    if (c1.get().isNullIV() && c2.get().isNullIV()) {
//                        
//                        if (c1.getProperty(Constant.Annotations.VAR) == c2
//                                .getProperty(Constant.Annotations.VAR)) {
//                        
//                            // Two MockIVs associated with the same variable.
//                            return;
//                            
//                        }
//                        
//                    }
//
//                }
                
                fail("Expecting: " + o1 + ", actual=" + o2);
                
            }
            
        }
        
        if (!o1.equals(o2)) {

//            o1.equals(o2); // debug point.
            
            fail("Failed to detect difference reported by equals(): expected="
                    + o1 + ", actual=" + o2);
            
        }
        
    }

    /**
	 * Utility method compares expected and actual solutions and reports on any
	 * discrepancies.
	 * 
	 * @param name
	 *            The name of the test.
	 * @param testURI
	 *            The URI for the test.
	 * @param store
	 *            The {@link AbstractTripleStore} (optional).
	 * @param astContainer
	 *            The {@link ASTContainer} (optional).
	 * @param queryResult
	 *            The actual result.
	 * @param expectedResult
	 *            The expected result.
	 * @param laxCardinality
	 *            When <code>true</code>, strict cardinality will be enforced.
	 * @param checkOrder
	 *            When <code>true</code>, the order must be the same.
	 * @throws AssertionFailedError
	 *             if the results are not the same.
	 * @throws QueryEvaluationException
	 */
	static public void compareTupleQueryResults(
		final String name,//
		final String testURI, //
		final AbstractTripleStore store,//
		final ASTContainer astContainer,//
        final TupleQueryResult queryResult,//
        final TupleQueryResult expectedResult,//
        final boolean laxCardinality,//
		final boolean checkOrder//
	) throws QueryEvaluationException {

    /*
     * Create MutableTupleQueryResult to be able to re-iterate over the
     * results.
     */
    
    final MutableTupleQueryResult queryResultTable = new MutableTupleQueryResult(queryResult);
    
    final MutableTupleQueryResult expectedResultTable = new MutableTupleQueryResult(expectedResult);

    boolean resultsEqual;
    if (laxCardinality) {
        resultsEqual = QueryResultUtil.isSubset(queryResultTable, expectedResultTable);
    }
    else {
        resultsEqual = QueryResultUtil.equals(queryResultTable, expectedResultTable);
        
        if (checkOrder) {
            // also check the order in which solutions occur.
            queryResultTable.beforeFirst();
            expectedResultTable.beforeFirst();

            while (queryResultTable.hasNext()) {
                final BindingSet bs = queryResultTable.next();
                final BindingSet expectedBs = expectedResultTable.next();
                
                if (! bs.equals(expectedBs)) {
                    resultsEqual = false;
                    break;
                }
            }
        }
    }

    // Note: code block shows the expected and actual results.
    StringBuilder expectedAndActualResults = null;
    if (!resultsEqual && true) {
        queryResultTable.beforeFirst();
        expectedResultTable.beforeFirst();
        final StringBuilder message = new StringBuilder(2048);
        message.append("\n============ ");
        message.append(name);
        message.append(" =======================\n");
        message.append("Expected result [")
            .append(expectedResultTable.size())
            .append("] not equal to query result [")
            .append(queryResultTable.size())
            .append("] \n");
        message.append(" =======================\n");
        message.append("Expected result [").append(expectedResultTable.size()).append("]: \n");
        while (expectedResultTable.hasNext()) {
            message.append(expectedResultTable.next());
            message.append("\n");
        }
        message.append("=============");
        StringUtil.appendN('=', name.length(), message);
        message.append("========================\n");
        message.append("Query result [").append(queryResultTable.size()).append("]: \n");
        while (queryResultTable.hasNext()) {
            message.append(queryResultTable.next());
            message.append("\n");
        }
        message.append("=============");
        StringUtil.appendN('=', name.length(), message);
        message.append("========================\n");
        expectedAndActualResults = message;
//        log.error(message);
    }

    if (!resultsEqual) {

        queryResultTable.beforeFirst();
        expectedResultTable.beforeFirst();

        final List<BindingSet> queryBindings = Iterations.asList(queryResultTable);
        final List<BindingSet> expectedBindings = Iterations.asList(expectedResultTable);

        final List<BindingSet> missingBindings = new ArrayList<BindingSet>(expectedBindings);
        missingBindings.removeAll(queryBindings);

        final List<BindingSet> unexpectedBindings = new ArrayList<BindingSet>(queryBindings);
        unexpectedBindings.removeAll(expectedBindings);

        final StringBuilder message = new StringBuilder(2048);
        message.append("\n");
        message.append(testURI);
        message.append("\n");
        message.append(name);
        message.append("\n===================================\n");

        if (!missingBindings.isEmpty()) {

            message.append("Missing bindings: \n");
            for (BindingSet bs : missingBindings) {
                message.append(bs);
                message.append("\n");
            }

            message.append("=============");
            StringUtil.appendN('=', name.length(), message);
            message.append("========================\n");
        }

        if (!unexpectedBindings.isEmpty()) {
            message.append("Unexpected bindings: \n");
            for (BindingSet bs : unexpectedBindings) {
                message.append(bs);
                message.append("\n");
            }

            message.append("=============");
            StringUtil.appendN('=', name.length(), message);
            message.append("========================\n");
        }
        
        if (checkOrder && missingBindings.isEmpty() && unexpectedBindings.isEmpty()) {
            message.append("Results are not in expected order.\n");
            message.append(" =======================\n");
            message.append("query result: \n");
            for (BindingSet bs: queryBindings) {
                message.append(bs);
                message.append("\n");
            }
            message.append(" =======================\n");
            message.append("expected result: \n");
            for (BindingSet bs: expectedBindings) {
                message.append(bs);
                message.append("\n");
            }
            message.append(" =======================\n");

            log.error(message.toString());
        }

        if (expectedAndActualResults != null) {
            message.append(expectedAndActualResults);
        }
        
//            RepositoryConnection con = ((DatasetRepository)dataRep).getDelegate().getConnection();
//            System.err.println(con.getClass());
//            try {
        if(astContainer!=null) {
            message.append("\n===================================\n");
            message.append(astContainer.toString());
    }
        if(store!=null&&
            store.getStatementCount()<100) {
            message.append("\n===================================\n");
            message.append("database dump:\n");
            message.append(store.dumpStore());
            }
//                RepositoryResult<Statement> stmts = con.getStatements(null, null, null, false);
//                while (stmts.hasNext()) {
//                    message.append(stmts.next());
//                    message.append("\n");
//                }
//            } finally {
//                con.close();
//            }
            
        log.error(message.toString());
        fail(message.toString());
    }
        /* debugging only: print out result when test succeeds 
        else {
            queryResultTable.beforeFirst();

            List<BindingSet> queryBindings = Iterations.asList(queryResultTable);
            StringBuilder message = new StringBuilder(128);

            message.append("\n============ ");
            message.append(getName());
            message.append(" =======================\n");

            message.append(" =======================\n");
            message.append("query result: \n");
            for (BindingSet bs: queryBindings) {
                message.append(bs);
                message.append("\n");
            }
            
            System.out.print(message.toString());
        }
        */
    }

	/**
	 * Compare the expected and actual query results for a graph query.
	 * 
	 * @param name
	 *            The name of the test.
	 * @param queryResult
	 *            The actual result.
	 * @param expectedResult
	 *            The expected result.
	 * 
	 * @throws AssertionFailedError
	 *             if the results are not the same.
	 */
	static public void compareGraphs(final String name,
			final Set<Statement> queryResult,
			final Set<Statement> expectedResult) {

		if (!ModelUtil.equals(expectedResult, queryResult)) {
			// Don't use RepositoryUtil.difference, it reports incorrect diffs
			/*
			 * Collection<? extends Statement> unexpectedStatements =
			 * RepositoryUtil.difference(queryResult, expectedResult);
			 * Collection<? extends Statement> missingStatements =
			 * RepositoryUtil.difference(expectedResult, queryResult);
			 * StringBuilder message = new StringBuilder(128);
			 * message.append("\n=======Diff: "); message.append(getName());
			 * message.append("========================\n"); if
			 * (!unexpectedStatements.isEmpty()) { message.append("Unexpected
			 * statements in result: \n"); for (Statement st :
			 * unexpectedStatements) { message.append(st.toString());
			 * message.append("\n"); } message.append("============="); for (int
			 * i = 0; i < getName().length(); i++) { message.append("="); }
			 * message.append("========================\n"); } if
			 * (!missingStatements.isEmpty()) { message.append("Statements
			 * missing in result: \n"); for (Statement st : missingStatements) {
			 * message.append(st.toString()); message.append("\n"); }
			 * message.append("============="); for (int i = 0; i <
			 * getName().length(); i++) { message.append("="); }
			 * message.append("========================\n"); }
			 */
			StringBuilder message = new StringBuilder(128);
			message.append("\n============ ");
			message.append(name);
			message.append(" =======================\n");
			message.append("Expected result: \n");
			for (Statement st : expectedResult) {
				message.append(st.toString());
				message.append("\n");
			}
			message.append("=============");
			StringUtil.appendN('=', name.length(), message);
			message.append("========================\n");

			message.append("Query result: \n");
			for (Statement st : queryResult) {
				message.append(st.toString());
				message.append("\n");
			}
			message.append("=============");
			StringUtil.appendN('=', name.length(), message);
			message.append("========================\n");

			log.error(message.toString());
			fail(message.toString());
		}
	}
        
}
