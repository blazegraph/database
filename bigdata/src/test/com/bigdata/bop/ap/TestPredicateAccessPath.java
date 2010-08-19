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

import java.util.Iterator;
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
import com.bigdata.striterator.ChunkedArrayIterator;

/**
 * Unit test for reading on an access path using a {@link Predicate}.
 * 
 * @todo layering in {@link IElementFilter}s.
 * @todo layering in a {@link DistinctElementFilter} filter.
 * @todo verify the correct reporting of access path statistics.
 * @todo write unit tests for the {@link Union} operator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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

    public Properties getProperties() {

        final Properties p = super.getProperties();

        p.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Transient
                .toString());

        return p;

    }

    /**
     * This unit test works through the create and population of a test relation
     * with some data and verifies the ability to access that data using some
     * different access paths. This sets the ground for testing the evaluation
     * of {@link Predicate}s with various constraints, filters, etc.
     */
    public void test_scan() {

        final String namespace = "ns";

        final Properties properties = getProperties();

        final Journal store = new Journal(properties);

        try {

            /*
             * Create and populate the relation.
             */
            {
                // create the relation.
                final R rel = new R(store, namespace, ITx.UNISOLATED,
                        properties);
                rel.create();

                // data to insert.
                final E[] a = {//
                        new E("John", "Mary"),// 
                        new E("Mary", "Paul"),// 
                        new E("Paul", "Leon"),// 
                        new E("Leon", "Paul"),// 
                };

                // insert data (the records are not pre-sorted).
                rel
                        .insert(new ChunkedArrayIterator<E>(a.length, a, null/* keyOrder */));

                // Do commit since not scale-out.
                store.commit();
                
            }
            
            /*
             * Verify read back.
             */
            {
                // should exist as of the last commit point.
                final R rel = (R) store.getResourceLocator().locate(namespace,
                        ITx.READ_COMMITTED);
                
                assertNotNull(rel);

                /*
                 * using a predicate with nothing bound, verify that we get the
                 * right range count on the relation.
                 */
                {

                    // nothing bound.
                    final IAccessPath<E> accessPath = rel
                            .getAccessPath(new Predicate<E>(
                                    new IVariableOrConstant[] {
                                            Var.var("name"), Var.var("value") },
                                    namespace));

                    assertEquals(4, accessPath.rangeCount(true/* exact */));

                    // now write out the elements visited by that access path.
                    System.out.println("accessPath="+accessPath);
                    final Iterator<E> itr = accessPath.iterator();
                    int n = 0;
                    while(itr.hasNext()) {
                        System.out.println(n + " : " + itr.next());
                        n++;
                    }
                    
                }

                /*
                 * using a predicate which binds the [name] position, verify
                 * that we get the right range count on the relation and verify
                 * the actual element pulled back from the access path.
                 */
                {

                    final IAccessPath<E> accessPath = rel
                            .getAccessPath(new Predicate<E>(
                                    new IVariableOrConstant[] {
                                            new Constant<String>("Mary"),
                                            Var.var("value") }, namespace));

                    assertEquals(1, accessPath.rangeCount(true/* exact */));
                    
                    // now write out the elements visited by that access path.
                    System.out.println("accessPath="+accessPath);
                    final Iterator<E> itr = accessPath.iterator();
                    int n = 0;
                    while(itr.hasNext()) {
                        System.out.println(n + " : " + itr.next());
                        n++;
                    }

                }

                /*
                 * @todo setup and run bops against it w/ filters, sorts, joins
                 * (we can join with an incoming binding set easily enough using
                 * only a single primary index), distincts, selecting only
                 * certain columns, etc.
                 */
                
            }

            fail("implement");
            
        } finally {

            store.destroy();
            
        }

    }
    
}
