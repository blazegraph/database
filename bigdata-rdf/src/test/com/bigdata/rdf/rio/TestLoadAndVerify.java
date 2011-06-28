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
 * Created on Oct 23, 2007
 */

package com.bigdata.rdf.rio;

import java.io.File;
import java.util.Properties;

import org.openrdf.model.Statement;

import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.rdf.load.IStatementBufferFactory;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test loads an RDF/XML resource into a database and then verifies by re-parse
 * that all expected statements were made persistent in the database.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLoadAndVerify extends AbstractRIOTestCase {

    /**
     * 
     */
    public TestLoadAndVerify() {
    }

    /**
     * @param name
     */
    public TestLoadAndVerify(String name) {
        super(name);
    }
    
    /**
     * @todo observed exception with verify in parallel which suggests a problem
     *       with {@link UnisolatedReadWriteIndex}
     * 
     * <pre>
     * java.lang.RuntimeException: java.util.concurrent.ExecutionException: java.lang.IllegalArgumentException: Not our child : child=com.bigdata.btree.Leaf@1f0cf51#1165593256395406
     *     at com.bigdata.rdf.rio.AbstractRIOTestCase.doVerify(AbstractRIOTestCase.java:470)
     *     at com.bigdata.rdf.rio.AbstractRIOTestCase.doLoadAndVerifyTest(AbstractRIOTestCase.java:101)
     *     at com.bigdata.rdf.rio.TestLoadAndVerify.test_loadAndVerify_U1(TestLoadAndVerify.java:115)
     * ...
     * Caused by: java.lang.IllegalArgumentException: Not our child : child=com.bigdata.btree.Leaf@1f0cf51#1165593256395406
     *     at com.bigdata.btree.Node.getIndexOf(Node.java:1826)
     *     at com.bigdata.btree.Node.getRightSibling(Node.java:1734)
     *     at com.bigdata.btree.BTree$LeafCursor.next(BTree.java:2035)
     *     at com.bigdata.btree.AbstractBTreeTupleCursor$AbstractCursorPosition.nextLeaf(AbstractBTreeTupleCursor.java:1508)
     *     at com.bigdata.btree.AbstractBTreeTupleCursor$MutableCursorPosition.nextLeaf(AbstractBTreeTupleCursor.java:2035)
     *     at com.bigdata.btree.AbstractBTreeTupleCursor$AbstractCursorPosition.forwardScan(AbstractBTreeTupleCursor.java:1766)
     *     at com.bigdata.btree.AbstractBTreeTupleCursor.nextTuple(AbstractBTreeTupleCursor.java:783)
     *     at com.bigdata.btree.AbstractBTreeTupleCursor.next(AbstractBTreeTupleCursor.java:809)
     *     at com.bigdata.btree.filter.Advancer$Advancerator.next(Advancer.java:116)
     *     at com.bigdata.btree.filter.Advancer$Advancerator.next(Advancer.java:1)
     *     at cutthecrap.utils.striterators.Striterator.next(Striterator.java:60)
     *     at com.bigdata.btree.filter.FilterConstructor$WrappedTupleIterator.next(FilterConstructor.java:99)
     *     at com.bigdata.btree.ResultSet.&lt;init&gt;(ResultSet.java:978)
     *     at com.bigdata.btree.ChunkedLocalRangeIterator.getResultSet(ChunkedLocalRangeIterator.java:140)
     *     at com.bigdata.btree.UnisolatedReadWriteIndex$ChunkedIterator.getResultSet(UnisolatedReadWriteIndex.java:719)
     *     at com.bigdata.btree.AbstractChunkedTupleIterator.rangeQuery(AbstractChunkedTupleIterator.java:305)
     *     at com.bigdata.btree.AbstractChunkedTupleIterator.hasNext(AbstractChunkedTupleIterator.java:427)
     *     at cutthecrap.utils.striterators.Resolverator.hasNext(Resolverator.java:48)
     *     at cutthecrap.utils.striterators.Striterator.hasNext(Striterator.java:55)
     *     at com.bigdata.striterator.ChunkedWrappedIterator.hasNext(ChunkedWrappedIterator.java:197)
     *     at com.bigdata.rdf.store.AbstractTripleStore.predicateUsage(AbstractTripleStore.java:2379)
     *     at com.bigdata.rdf.store.AbstractTripleStore.predicateUsage(AbstractTripleStore.java:2345)
     *     at com.bigdata.rdf.rio.AbstractRIOTestCase$VerifyTask.verify(AbstractRIOTestCase.java:618)
     *     at com.bigdata.rdf.rio.AbstractRIOTestCase$VerifyTask.call(AbstractRIOTestCase.java:596)
     *     at com.bigdata.rdf.rio.AbstractRIOTestCase$VerifyTask.call(AbstractRIOTestCase.java:1)
     *     at java.util.concurrent.FutureTask$Sync.innerRun(FutureTask.java:303)
     *     at java.util.concurrent.FutureTask.run(FutureTask.java:138)
     *     at java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:886)
     *     at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:908)
     *     at java.lang.Thread.run(Thread.java:619)
     * 
     * </pre>
     */
    final boolean parallel = false;

    /**
     * Test with the "small.rdf" data set.
     * 
     * @throws Exception
     */
    public void test_loadAndVerify_small() throws Exception {
        
        final String resource = "bigdata-rdf/src/test/com/bigdata/rdf/rio/small.rdf";

        doLoadAndVerifyTest(resource, parallel);

    }

    /**
     * Test with the "sample data.rdf" data set.
     * 
     * @throws Exception
     */
    public void test_loadAndVerify_sampleData() throws Exception {
        
        final String resource = "bigdata-rdf/src/test/com/bigdata/rdf/rio/sample data.rdf";

        doLoadAndVerifyTest( resource, parallel );
        
    }
   
    /**
     * Uses a modest (40k statements) file.
     */
    public void test_loadAndVerify_modest() throws Exception {
        
//      final String file = "../rdf-data/nciOncology.owl";
//        final String file = "../rdf-data/alibaba_v41.rdf";
        final String file = "bigdata-rdf/src/resources/data/bsbm/dataset_pc100.nt";

        if (!new File(file).exists()) {

            fail("Resource not found: " + file + ", test skipped: " + getName());

            return;
            
        }

        doLoadAndVerifyTest( file, parallel );
        
    }

    /**
     * LUBM U(1)
     */
    public void test_loadAndVerify_U1() throws Exception {
        
        final String file = "bigdata-rdf/src/resources/data/lehigh/U1";

        doLoadAndVerifyTest(file, parallel);
        
    }

    /**
     * Note: This allows an override of the properties that effect the data
     * load, in particular whether or not the full text index and statement
     * identifiers are maintained. It can be useful to disable those features in
     * order to estimate the best load rate for a data set.
     */
    protected AbstractTripleStore getStore() {

        final Properties properties = new Properties(getProperties());

        // properties.setProperty(AbstractTripleStore.Options.TEXT_INDEX,
        // "false");
        //
        // properties.setProperty(
        // AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");

        return getStore(properties);

    }
    
    private static class StatementBufferFactory<S extends Statement> implements
            IStatementBufferFactory<S> {

        final AbstractTripleStore store;

        public StatementBufferFactory(final AbstractTripleStore store) {

            this.store = store;

        }

        public IStatementBuffer<S> newStatementBuffer() {

            return new StatementBuffer<S>(store, 100000/* capacity */);

        }
        
    }
    
    protected void doLoad(AbstractTripleStore store, String resource,
            final boolean parallel)
            throws Exception {
        
        doLoad(store, resource, parallel, new StatementBufferFactory<BigdataStatement>(store));
        
    }
    
//    /**
//     * Load the file using the {@link DataLoader}.
//     * <p>
//     * Note: Normally we disable closure for this test, but that is not
//     * critical. If you compute the closure of the data set then there will
//     * simply be additional statements whose self-consistency among the
//     * statement indices will be verified, but it will not verify the
//     * correctness of the closure.
//     */
//    protected void load(final AbstractTripleStore store, final String resource)
//            throws Exception {
//
//        // avoid modification of the properties.
//        final Properties properties = new Properties(getProperties());
//
//        // turn off RDFS closure for this test.
//        properties.setProperty(DataLoader.Options.CLOSURE, ClosureEnum.None
//                .toString());
//
//        final DataLoader dataLoader = new DataLoader(properties, store);
//
//        // load into the datbase.
//        dataLoader.loadData(resource, "" /* baseURI */, RDFFormat.RDFXML);
//
//        // // database-at-once closure (optional for this test).
//        // store.getInferenceEngine().computeClosure(null/*focusStore*/);
//
//    }

}
