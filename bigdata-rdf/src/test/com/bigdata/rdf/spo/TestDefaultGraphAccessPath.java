/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Oct 25, 2007
 */

package com.bigdata.rdf.spo;

import java.util.Arrays;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;

import com.bigdata.io.SerializerUtil;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;

/**
 * Test suite for {@link DefaultGraphSolutionExpander}. These tests are written
 * to the {@link SPOAccessPath}. While they do not strictly speaking presume
 * access to local indices, the {@link SPOAccessPath} is always executed against
 * local indices in practice since the pipeline joins distribute query
 * processing to the index partitions (shards) while the LDS mode runs query
 * inside of the embedded data service.
 * 
 * @todo write unit tests for {@link NamedGraphSolutionExpander}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDefaultGraphAccessPath extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestDefaultGraphAccessPath() {
        super();
    }

    /**
     * @param name
     */
    public TestDefaultGraphAccessPath(String name) {
        super(name);
    }

    /**
     * A series of tests for querying SPARQL default graphs. The expander should
     * apply the access path to the graph associated with each specified URI,
     * visiting the distinct (s,p,o) tuples found in those graph(s).
     * <p>
     * Note: The rangeCount(false) assertions have been commented out since the
     * method will report a different (higher) estimate if the access path is
     * configured to filter out statements outside of the desired context(s).
     */
    public void test_defaultGraphs() {
        
        final AbstractTripleStore store = getStore();

        try {
            
            if(!store.isQuads()) {
                
                log.warn("test requires quads.");

                return;
                
            }

            final BigdataValueFactory f = store.getValueFactory();
            
            final BigdataURI john = f.createURI("http://www.bigdata.com/john");
            final BigdataURI loves= f.createURI("http://www.bigdata.com/loves");
            final BigdataURI mary = f.createURI("http://www.bigdata.com/mary");
            final BigdataURI paul = f.createURI("http://www.bigdata.com/paul");
            final BigdataURI sam = f.createURI("http://www.bigdata.com/sam");
            final BigdataURI c1 = f.createURI("http://www.bigdata.com/context1");
            final BigdataURI c2 = f.createURI("http://www.bigdata.com/context2");
//            final BigdataURI c3 = f.createURI("http://www.bigdata.com/context3");
            final BigdataURI c4 = f.createURI("http://www.bigdata.com/context4");

            /*
             * Setup the data for the test.
             */
            {

                final StatementBuffer<Statement> buffer = new StatementBuffer<Statement>(
                        store, 10);

                // add values to the store, so term identifiers will be set.
                store.addTerms(new BigdataValue[] { john, loves, mary, paul,
                        c1, c2 });

                // add statements to the store.
                buffer.add(john, loves, mary, c1);
                buffer.add(mary, loves, paul, c2);
                buffer.add(paul, loves, sam, c4);
                buffer.flush();

            }

            assertTrue(c1.getIV() != null);
            assertTrue(c2.getIV() != null);
            assertTrue(c4.getIV() != null);

            assertTrue(store.hasStatement(john, loves, mary, c1));
            assertTrue(store.hasStatement(mary, loves, paul, c2));
            assertTrue(store.hasStatement(paul, loves, sam, c4));

            {
                
                /*
                 * No graphs in the defaultGraphs set.
                 */
                
                final DefaultGraphSolutionExpander expander = new DefaultGraphSolutionExpander(
                        Arrays
                                .asList(new BigdataURI[] {}/* defaultGraphs */));

                final SPOAccessPath baseAccessPath = (SPOAccessPath) store
                        .getAccessPath(SPOKeyOrder.SPOC);

                if (log.isInfoEnabled()) {
                    log.info(expander.getAccessPath(baseAccessPath).toString());
                }

                TestSPOKeyOrder.assertSameIteratorAnyOrder("iterator()",
                        new ISPO[] {}, expander.getAccessPath(baseAccessPath)
                                .iterator());

                assertTrue("isEmpty", expander.getAccessPath(baseAccessPath)
                        .isEmpty());

                assertEquals("rangeCount()", 0L, expander.getAccessPath(
                        baseAccessPath).rangeCount(false/* exact */));

                assertEquals("rangeCount(exact)", 0L, expander.getAccessPath(
                        baseAccessPath).rangeCount(true/* exact */));

            }

            {
                
                /*
                 * One graph in the defaultGraphs set.
                 * 
                 * Note: This case gets handled specially.
                 */
                
                final DefaultGraphSolutionExpander expander = new DefaultGraphSolutionExpander(
                        Arrays
                                .asList(new BigdataURI[] { c1 }/* defaultGraphs */));

                final SPOAccessPath baseAccessPath = (SPOAccessPath) store
                        .getAccessPath(SPOKeyOrder.SPOC);

                if (log.isInfoEnabled()) {
                    log.info(expander.getAccessPath(baseAccessPath).toString());
                }

                TestSPOKeyOrder.assertSameIteratorAnyOrder("iterator()",
                        new ISPO[] { //
                        new SPO(john.getIV(), loves.getIV(), mary
                                .getIV()) //
                        }, expander.getAccessPath(baseAccessPath)
                        .iterator());

                assertFalse("isEmpty", expander.getAccessPath(baseAccessPath)
                        .isEmpty());

//                assertEquals("rangeCount()", 1L, expander.getAccessPath(
//                        baseAccessPath).rangeCount(false/* exact */));

                assertEquals("rangeCount(exact)", 1L, expander.getAccessPath(
                        baseAccessPath).rangeCount(true/* exact */));

            }

            {

                /*
                 * Two graphs in the defaultGraphs set.
                 */

                final DefaultGraphSolutionExpander expander = new DefaultGraphSolutionExpander(
                        Arrays
                                .asList(new BigdataURI[] { c1, c2 }/* defaultGraphs */));

                final SPOAccessPath baseAccessPath = (SPOAccessPath) store
                        .getAccessPath(SPOKeyOrder.SPOC);

                if (log.isInfoEnabled()) {
                    log.info(expander.getAccessPath(baseAccessPath).toString());
                }

                TestSPOKeyOrder.assertSameIteratorAnyOrder("iterator()",
                        new ISPO[] { //
                        new SPO(john.getIV(), loves.getIV(), mary
                                .getIV()), //
                        new SPO(mary.getIV(), loves.getIV(), paul
                                .getIV()) //
                        }, expander.getAccessPath(baseAccessPath).iterator());

                assertFalse("isEmpty", expander.getAccessPath(baseAccessPath)
                        .isEmpty());

//                assertEquals("rangeCount()", 2L, expander.getAccessPath(
//                        baseAccessPath).rangeCount(false/* exact */));

                assertEquals("rangeCount(exact)", 2L, expander.getAccessPath(
                        baseAccessPath).rangeCount(true/* exact */));

            }

            {

                /*
                 * Query with more restricted predicate bindings (john is
                 * bound as the subject).
                 */

                final DefaultGraphSolutionExpander expander = new DefaultGraphSolutionExpander(
                        Arrays
                                .asList(new BigdataURI[] { c1, c2 }/* defaultGraphs */));

                final SPOAccessPath baseAccessPath = (SPOAccessPath) store
                        .getAccessPath(john, null, null, (Resource) null);

                if (log.isInfoEnabled()) {
                    log.info(expander.getAccessPath(baseAccessPath).toString());
                }

                TestSPOKeyOrder.assertSameIteratorAnyOrder("iterator()",
                        new ISPO[] { //
                        new SPO(john.getIV(), loves.getIV(), mary
                                .getIV()), //
                        }, expander.getAccessPath(baseAccessPath).iterator());

                assertFalse("isEmpty", expander.getAccessPath(baseAccessPath)
                        .isEmpty());

//                assertEquals("rangeCount()", 1L, expander.getAccessPath(
//                        baseAccessPath).rangeCount(false/* exact */));

                assertEquals("rangeCount(exact)", 1L, expander.getAccessPath(
                        baseAccessPath).rangeCount(true/* exact */));

            }

            {

                /*
                 * Query with more restricted predicate bindings (mary is
                 * bound as the subject).
                 */

                final DefaultGraphSolutionExpander expander = new DefaultGraphSolutionExpander(
                        Arrays
                                .asList(new BigdataURI[] { c1, c2 }/* defaultGraphs */));

                final SPOAccessPath baseAccessPath = (SPOAccessPath) store
                        .getAccessPath(mary, null, null, (Resource) null);

                if (log.isInfoEnabled()) {
                    log.info(expander.getAccessPath(baseAccessPath).toString());
                }

                TestSPOKeyOrder.assertSameIteratorAnyOrder("iterator()",
                        new ISPO[] { //
                        new SPO(mary.getIV(), loves.getIV(), paul
                                .getIV()), //
                        }, expander.getAccessPath(baseAccessPath).iterator());

                assertFalse("isEmpty", expander.getAccessPath(baseAccessPath)
                        .isEmpty());

//                assertEquals("rangeCount()", 1L, expander.getAccessPath(
//                        baseAccessPath).rangeCount(false/* exact */));

                assertEquals("rangeCount(exact)", 1L, expander.getAccessPath(
                        baseAccessPath).rangeCount(true/* exact */));

            }

            {

                /*
                 * Query with more restricted predicate bindings (mary is
                 * bound as the object).
                 */

                final DefaultGraphSolutionExpander expander = new DefaultGraphSolutionExpander(
                        Arrays
                                .asList(new BigdataURI[] { c1, c2 }/* defaultGraphs */));

                final SPOAccessPath baseAccessPath = (SPOAccessPath) store
                        .getAccessPath(null, null, mary, (Resource) null);

                if (log.isInfoEnabled()) {
                    log.info(expander.getAccessPath(baseAccessPath).toString());
                }

                TestSPOKeyOrder.assertSameIteratorAnyOrder("iterator()",
                        new ISPO[] { //
                        new SPO(john.getIV(), loves.getIV(), mary
                                .getIV()), //
                        }, expander.getAccessPath(baseAccessPath).iterator());

                assertFalse("isEmpty", expander.getAccessPath(baseAccessPath)
                        .isEmpty());

//                assertEquals("rangeCount()", 1L, expander.getAccessPath(
//                        baseAccessPath).rangeCount(false/* exact */));

                assertEquals("rangeCount(exact)", 1L, expander.getAccessPath(
                        baseAccessPath).rangeCount(true/* exact */));

            }

            {

                /*
                 * Query with more restricted predicate bindings (john is bound
                 * as the subject and paul is bound as the object, so there are
                 * no solutions).
                 */

                final DefaultGraphSolutionExpander expander = new DefaultGraphSolutionExpander(
                        Arrays
                                .asList(new BigdataURI[] { c1, c2 }/* defaultGraphs */));

                final SPOAccessPath baseAccessPath = (SPOAccessPath) store
                        .getAccessPath(john, null, paul, (Resource) null);

                if (log.isInfoEnabled()) {
                    log.info(expander.getAccessPath(baseAccessPath).toString());
                }

                TestSPOKeyOrder.assertSameIteratorAnyOrder("iterator()",
                        new ISPO[] {}, expander.getAccessPath(baseAccessPath)
                                .iterator());

                assertTrue("isEmpty", expander.getAccessPath(baseAccessPath)
                        .isEmpty());

                assertEquals("rangeCount()", 0L, expander.getAccessPath(
                        baseAccessPath).rangeCount(false/* exact */));

                assertEquals("rangeCount(exact)", 0L, expander.getAccessPath(
                        baseAccessPath).rangeCount(true/* exact */));

            }

            {

                /*
                 * Default graph is null.
                 * 
                 * Note: This case gets handled specially.
                 */

                final DefaultGraphSolutionExpander expander = new DefaultGraphSolutionExpander(
                        null/* defaultGraphs */);

                final SPOAccessPath baseAccessPath = (SPOAccessPath) store
                        .getAccessPath(SPOKeyOrder.SPOC);

                if (log.isInfoEnabled()) {
                    log.info(expander.getAccessPath(baseAccessPath).toString());
                }

                TestSPOKeyOrder.assertSameIteratorAnyOrder("iterator()",
                        new ISPO[] { //
                                new SPO(john.getIV(), loves.getIV(),
                                        mary.getIV()), //
                                new SPO(mary.getIV(), loves.getIV(),
                                        paul.getIV()), //
                                new SPO(paul.getIV(), loves.getIV(),
                                        sam.getIV()) //
                        }, expander.getAccessPath(baseAccessPath).iterator());

                assertFalse("isEmpty", expander.getAccessPath(baseAccessPath)
                        .isEmpty());

//                assertEquals("rangeCount()", 2L, expander.getAccessPath(
//                        baseAccessPath).rangeCount(false/* exact */));

                assertEquals("rangeCount(exact)", 3L, expander.getAccessPath(
                        baseAccessPath).rangeCount(true/* exact */));

            }

            // Serialization test.
            {
                
                final DefaultGraphSolutionExpander expected = new DefaultGraphSolutionExpander(
                        Arrays
                                .asList(new BigdataURI[] { c1, c2 }/* defaultGraphs */));

                final DefaultGraphSolutionExpander actual = (DefaultGraphSolutionExpander) SerializerUtil
                        .deserialize(SerializerUtil.serialize(expected));

                assertEquals(actual.getKnownGraphCount(), expected.getKnownGraphCount());

                assertSameIterator(new BigdataURI[] { c1, c2 }, actual.getGraphs());
                
            }
            
        } finally {

            store.destroy();

        }

    }

}
