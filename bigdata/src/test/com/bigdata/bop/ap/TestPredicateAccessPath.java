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
 * Created on Aug 19, 2010
 */

package com.bigdata.bop.ap;

import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.Var;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultRuleTaskFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.NOPEvaluationPlanFactory;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Unit test for reading on an access path using a {@link Predicate}. This unit
 * test works through the create and population of a test relation with some
 * data and verifies the ability to access that data using some different access
 * paths. This sets the ground for testing the evaluation of {@link Predicate}s
 * with various constraints, filters, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestPredicateAccessPath.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 */
public class TestPredicateAccessPath extends TestCase2 {

    /**
     * 
     */
    public TestPredicateAccessPath() {
    }

    /**
     * @param name
     */
    public TestPredicateAccessPath(String name) {
        super(name);
    }

    @Override
    public Properties getProperties() {

        final Properties p = new Properties(super.getProperties());

        p.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Transient
                .toString());

        return p;
        
    }

    static private final String namespace = "ns";
    
    Journal jnl;
    
    R rel;
    
    public void setUp() throws Exception {
        
        jnl = new Journal(getProperties());

        loadData(jnl);

    }
    
    /**
     * Create and populate relation in the {@link #namespace}.
     */
    private void loadData(final Journal store) {

        // create the relation.
        final R rel = new R(store, namespace, ITx.UNISOLATED, new Properties());
        rel.create();

        // data to insert.
        final E[] a = {//
                new E("John", "Mary"),// 
                new E("Mary", "Paul"),// 
                new E("Paul", "Leon"),// 
                new E("Leon", "Paul"),// 
                new E("Mary", "John"),// 
        };

        // insert data (the records are not pre-sorted).
        rel.insert(new ChunkedArrayIterator<E>(a.length, a, null/* keyOrder */));

        // Do commit since not scale-out.
        store.commit();

        // should exist as of the last commit point.
        this.rel = (R) jnl.getResourceLocator().locate(namespace,
                ITx.READ_COMMITTED);

        assertNotNull(rel);
    
    }

    public void tearDown() throws Exception {

        if (jnl != null) {
            jnl.destroy();
            jnl = null;
        }
        
        // clear reference.
        rel = null;

    }

    /**
     * Using a predicate with nothing bound, verify that we get the
     * right range count on the relation and that we read the correct elements
     * from the relation.
     */
    public void test_nothingBound() {

        // nothing bound.
        final IAccessPath<E> accessPath = rel
                .getAccessPath(new Predicate<E>(new IVariableOrConstant[] {
                        Var.var("name"), Var.var("value") }, namespace));

        // verify the range count.
        assertEquals(5, accessPath.rangeCount(true/* exact */));

        // visit that access path, verifying the elements and order.
        if (log.isInfoEnabled())
            log.info("accessPath=" + accessPath);
        final E[] expected = new E[] {//
                new E("John", "Mary"),// 
                new E("Leon", "Paul"),// 
                new E("Mary", "John"),//
                new E("Mary", "Paul"),// 
                new E("Paul", "Leon"),//
        };
        final IChunkedOrderedIterator<E> itr = accessPath.iterator();
        try {
            int n = 0;
            while (itr.hasNext()) {
                final E e = itr.next();
                if (log.isInfoEnabled())
                    log.info(n + " : " + e);
                assertEquals(expected[n], e);
                n++;
            }
            assertEquals(expected.length,n);
        } finally {
            itr.close();
        }

    }

    /**
     * Using a predicate which binds the [name] position, verify that we get the
     * right range count on the relation and verify the actual element pulled
     * back from the access path.
     */
    public void test_scan() {

        final IAccessPath<E> accessPath = rel.getAccessPath(new Predicate<E>(
                new IVariableOrConstant[] { new Constant<String>("Mary"),
                        Var.var("value") }, namespace));

        // verify the range count.
        assertEquals(2, accessPath.rangeCount(true/* exact */));

        // visit that access path, verifying the elements and order.
        if (log.isInfoEnabled())
            log.info("accessPath=" + accessPath);
        final E[] expected = new E[] {//
                new E("Mary", "John"),// 
                new E("Mary", "Paul"),// 
        };
        final IChunkedOrderedIterator<E> itr = accessPath.iterator();
        try {
            int n = 0;
            while (itr.hasNext()) {
                final E e = itr.next();
                if (log.isInfoEnabled())
                    log.info(n + " : " + e);
                assertEquals(expected[n], e);
                n++;
            }
            assertEquals(expected.length,n);
        } finally {
            itr.close();
        }

    }

    /**
     * Verify the Predicate evaluation using an {@link IJoinNexus}.
     * 
     * @deprecated This unit test will go away once we migrate away from the
     *             {@link IJoinNexus}. The {@link Predicate} will be evaluated
     *             by the join operator as it applies the appropriate bindings.
     */
    public void test_predicate_eval() {

        final IJoinNexusFactory joinNexusFactory = new MockJoinNexusFactory(
                ActionEnum.Query,//
                ITx.UNISOLATED,// writeTimestamp
                ITx.READ_COMMITTED,// readTimestamp,
                new Properties(),// ,
                IJoinNexus.ALL,// solutionFlags,
                null,// solutionFilter,
                NOPEvaluationPlanFactory.INSTANCE,//
                DefaultRuleTaskFactory.PIPELINE//
        );

        final Predicate<E> pred = new Predicate<E>(new IVariableOrConstant[] {
                new Constant<String>("Mary"), Var.var("value") }, namespace);

        final E[] expected = new E[] {//
                new E("Mary", "John"),// 
                new E("Mary", "Paul"),// 
        };
        final IChunkedOrderedIterator<E> itr = pred.eval(null/* fed */,
                joinNexusFactory.newInstance(jnl));
        try {
            int n = 0;
            while (itr.hasNext()) {
                final E e = itr.next();
                if (log.isInfoEnabled())
                    log.info(n + " : " + e);
                assertEquals(expected[n], e);
                n++;
            }
            assertEquals(expected.length,n);
        } finally {
            itr.close();
        }

    }

    /**
     * 
     * @todo layering in {@link IElementFilter}s.
     * 
     * @todo layering in a {@link DistinctElementFilter} filter.
     */
    public void test_filter() {

//        final IAccessPath<E> accessPath = rel.getAccessPath(new Predicate<E>(
//                new IVariableOrConstant[] { new Constant<String>("Mary"),
//                        Var.var("value") }, namespace));
//
//        // verify the range count.
//        assertEquals(2, accessPath.rangeCount(true/* exact */));
//
//        // visit that access path, verifying the elements and order.
//        if (log.isInfoEnabled())
//            log.info("accessPath=" + accessPath);
//        final E[] expected = new E[] {//
//        new E("Mary", "Paul"),// 
//        };
//        final IChunkedOrderedIterator<E> itr = accessPath.iterator();
//        try {
//            int n = 0;
//            while (itr.hasNext()) {
//                final E e = itr.next();
//                if (log.isInfoEnabled())
//                    log.info(n + " : " + e);
//                assertEquals(expected[n], e);
//                n++;
//            }
//            assertEquals(expected.length, n);
//        } finally {
//            itr.close();
//        }

        fail("write test");
        
    }

}
