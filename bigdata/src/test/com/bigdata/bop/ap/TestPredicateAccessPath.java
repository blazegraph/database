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

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.Predicate.Annotations;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.MockRunningQuery;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Unit test for reading on an access path using a {@link Predicate}. This unit
 * test works through the create and population of a test relation with some
 * data and verifies the ability to access that data using some different access
 * paths. This sets the ground for testing the evaluation of {@link Predicate}s
 * with various constraints, filters, etc.
 * <p>
 * Note: Tests of remote access path reads are done in the context of a bigdata
 * federation since there must be a data service in play for a remote access
 * path.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestPredicateAccessPath.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 * 
 * @todo test read-committed access paths.
 * @todo test read historical access paths.
 * @todo test unisolated (writable) access paths.
 * @todo test fully isolated access paths.
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
     * Verify lookup and read on an {@link IPredicate}.
     */
    public void test_predicate_eval() {

        final Predicate<E> pred = new Predicate<E>(new IVariableOrConstant[] {
                new Constant<String>("Mary"), Var.var("value") }, 
                NV.asMap(new NV[] {//
                        new NV(Annotations.RELATION_NAME,new String[]{namespace}),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                        new NV(Annotations.REMOTE_ACCESS_PATH, false),//
                }));

        final E[] expected = new E[] {//
                new E("Mary", "John"),// 
                new E("Mary", "Paul"),// 
        };
        
        final BOpStats statIsIgnored = new BOpStats();

        final IAsynchronousIterator<IBindingSet[]> sourceIsIgnored = new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[0] });

        final IBlockingBuffer<IBindingSet[]> sinkIsIgnored = new BlockingBuffer<IBindingSet[]>(
                1/* capacity */);

        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                new MockRunningQuery(null/* fed */, jnl/* indexManager */
                ), -1/* partitionId */, statIsIgnored,
                sourceIsIgnored, sinkIsIgnored, null/* sink2 */);

        // lookup relation
        final R relation = (R) context.getRelation(pred);

        // obtain access path for that relation.
        final IAccessPath<E> ap = context.getAccessPath(relation, pred);
        
        // obtain range count from the access path.
        assertEquals(2L, ap.rangeCount(true/* exact */));
        
        // verify the data visited by the access path.
        final IChunkedOrderedIterator<E> itr = ap.iterator();
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

        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        final Predicate<E> pred = new Predicate<E>(new IVariableOrConstant[] {
                x, y }, NV.asMap(new NV[] {//
                        new NV(Annotations.RELATION_NAME,new String[]{namespace}),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                        new NV(Annotations.REMOTE_ACCESS_PATH, false),//
                        new NV(Annotations.CONSTRAINT, new IElementFilter<E>() {
                            
                            public boolean canAccept(Object o) {
                                return true;
                            }
                            
                            public boolean accept(E e) {
                                return e.name.equals("Mary");
                            }
                        })
                }));

        final E[] expected = new E[] {//
                new E("Mary", "John"),// 
                new E("Mary", "Paul"),// 
        };
        
        final BOpStats statIsIgnored = new BOpStats();

        final IAsynchronousIterator<IBindingSet[]> sourceIsIgnored = new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[0] });

        final IBlockingBuffer<IBindingSet[]> sinkIsIgnored = new BlockingBuffer<IBindingSet[]>(
                1/* capacity */);

        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                new MockRunningQuery(null/* fed */, jnl/* indexManager */
                ), -1/* partitionId */, statIsIgnored,
                sourceIsIgnored, sinkIsIgnored, null/* sink2 */);

        // lookup relation
        final R relation = (R) context.getRelation(pred);

        // obtain access path for that relation.
        final IAccessPath<E> ap = context.getAccessPath(relation, pred);
        
        // obtain range count from the access path.
        assertEquals(2L, ap.rangeCount(true/* exact */));
        
        // verify the data visited by the access path.
        final IChunkedOrderedIterator<E> itr = ap.iterator();
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

}
