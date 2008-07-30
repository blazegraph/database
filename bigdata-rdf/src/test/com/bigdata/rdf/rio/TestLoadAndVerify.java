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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.OptimizedValueFactory;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;
import com.bigdata.relation.accesspath.AbstractArrayBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;

/**
 * Test loads an RDF/XML resource into a database and then verifies by re-parse
 * that all expected statements were made persistent in the database.
 * 
 * @todo this test will probably fail if the source data contains bnodes since
 *       it does not validate bnodes based on consistent RDF properties but only
 *       based on their Java fields.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLoadAndVerify extends AbstractTripleStoreTestCase {

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
     * Test with the "small.rdf" data set.
     * 
     * @throws Exception
     */
    public void test_loadAndVerify_small() throws Exception {
        
        final String file = "src/test/com/bigdata/rdf/rio/small.rdf";

        doLoadAndVerifyTest( file );
        
    }

    /**
     * Test with the "sample data.rdf" data set.
     * 
     * @throws Exception
     */
    public void test_loadAndVerify_sampleData() throws Exception {
        
        final String file = "src/test/com/bigdata/rdf/rio/sample data.rdf";

        doLoadAndVerifyTest( file );
        
    }
   
    /**
     * @todo use some modest to largish RDF/XML that we can pack with the
     *       distribution.
     */
    public void test_loadAndVerify_modest() throws Exception {
        
//      final String file = "../rdf-data/nciOncology.owl";
        final String file = "../rdf-data/alibaba_v41.rdf";
        
        if(!new File(file).exists()) {
            
            fail("Resource not found: "+file+", test skipped: "+getName());
            
            return;
            
        }

        doLoadAndVerifyTest( file );
        
    }
   
    /**
     * Test loads an RDF/XML resource into a database and then verifies by
     * re-parse that all expected statements were made persistent in the
     * database. 
     * 
     * @param file
     * 
     * @throws Exception
     */
    protected void doLoadAndVerifyTest(String file) throws Exception {

        assertTrue("File not found? file=" + file, new File(file).exists());

        AbstractTripleStore store = getStore();

        try {

            // if(store instanceof ScaleOutTripleStore) {
            //
            // /*
            // * FIXME The test needs to use the batch API to verify the
            // * statements in the scale-out triple store.
            // */
            // fail("Test needs batch verification method for data service
            // versions.");
            //              
            // }

            /* Load the file. */
            {

                // avoid modification of the properties.
                final Properties properties = new Properties(getProperties());

                // turn off RDFS closure for this test.
                properties.setProperty(DataLoader.Options.CLOSURE,
                        ClosureEnum.None.toString());

                final DataLoader dataLoader = new DataLoader(properties, store);

                // load into the datbase.
                dataLoader.loadData(file, "" /* baseURI */, RDFFormat.RDFXML);

            }

            store.commit();

            if (store.isStable()) {

                store = reopenStore(store);

            }

            if (log.isInfoEnabled())
                log.info("\n" + store.predicateUsage());

            /*
             * re-parse and verify all statements exist in the db using each
             * statement index.
             */
            final AtomicInteger nerrs = new AtomicInteger(0);
            {

                final IRioLoader loader = new StatementVerifier(store, nerrs);

                loader.loadRdf(new BufferedReader(new InputStreamReader(
                        new FileInputStream(file))), ""/* baseURI */,
                        RDFFormat.RDFXML, false/* verify */);

            }

            assertEquals("nerrors", 0, nerrs.get());

            assertStatementIndicesConsistent(store);

        } finally {

            store.closeAndDelete();

        }

    }

    protected class StatementVerifier extends BasicRioLoader {

        final private AbstractTripleStore db;

        final private AtomicInteger nerrs;
        
        final private int MAX_ERRORS = 20;

        final IBuffer<Statement> buffer;
        
        public StatementVerifier(AbstractTripleStore db, AtomicInteger nerrs) {

            this.db = db;

            this.nerrs = nerrs;

            this.buffer = new AbstractArrayBuffer<Statement>(
                    10000/* capacity */, null/*filter*/) {

                @Override
                protected long flush(int n, Statement[] a) {

                    verifyStatements( n , a );
                    
                    return n;

                }
                
            };

        }

        /**
         * Report an error.
         * 
         * @param msg
         *            The error message.
         */
        private void error(String msg) {

            log.error(msg);
            
            if (nerrs.incrementAndGet() > MAX_ERRORS) {

                throw new RuntimeException("Too many errors");

            }

        }
        
        public RDFHandler newRDFHandler() {

            return new RDFHandlerBase() {

                public void handleStatement(Statement stmt) {
                    
                    buffer.add(stmt);
                    
                }

            };

        }

        private void verifyStatements(int n, Statement[] a) {

            final HashMap<Value,_Value> termSet = new HashMap<Value,_Value>(n);
            {

                for (Statement stmt : a) {

                    termSet.put(stmt.getSubject(),OptimizedValueFactory.INSTANCE
                            .toNativeValue(stmt.getSubject()));

                    termSet.put(stmt.getPredicate(),OptimizedValueFactory.INSTANCE
                            .toNativeValue(stmt.getPredicate()));

                    termSet.put(stmt.getObject(),OptimizedValueFactory.INSTANCE
                            .toNativeValue(stmt.getObject()));

                }

                final int nterms = termSet.size();

                final _Value[] terms = new _Value[nterms];

                int i = 0;
                for (_Value term : termSet.values()) {

                    terms[i++] = term;

                }

                db.getLexiconRelation()
                        .addTerms(terms, nterms, true/* readOnly */);

                int nunknown = 0;
                for (_Value term : terms) {

                    if (term.termId == 0L) {

                        error("Unknown term: " + term);
                        
                        nunknown++;

                    }

                }

                if (nunknown > 0) {
                    
                    log.warn("" + nunknown + " out of " + nterms
                            + " terms were not found.");
                    
                }
                
            }
            
            /*
             * Now verify reverse lookup for those terms.
             */
            {
                
                final HashSet<Long> ids  = new HashSet<Long>(termSet.size());
                
                for(_Value term : termSet.values()) {
                    
                    final long id = term.termId;
                    
                    if (id == NULL) {

                        // ignore terms that we know were not found.
                        continue;
                        
                    }
                    
                    ids.add(id);
                    
                }

                // batch resolve ids to terms.
                final Map<Long,BigdataValue> reverseMap = db.getLexiconRelation().getTerms(ids);
                
                for(_Value expectedTerm : termSet.values()) {
                    
                    final long id = expectedTerm.termId;
                    
                    if (id == NULL) {

                        // ignore terms that we know were not found.
                        continue;
                        
                    }

                    final BigdataValue actualTerm = reverseMap.get(id);

                    if (actualTerm == null || !actualTerm.equals(expectedTerm)) {

                        error("expectedTerm=" + expectedTerm
                                        + ", assigned termId=" + id
                                        + ", but reverse lookup reports: "
                                        + actualTerm);
                        
                    }
                    
                }
                
            }
            
// Resource s = stmt.getSubject();
// URI p = stmt.getPredicate();
//            Value o = stmt.getObject();
//            Resource c = stmt.getContext();
//
//            // convert to Sesame objects.
//            s = (Resource) db.asValue(s);
//            p = (URI) db.asValue(p);
//            o = (Value) db.asValue(o);
//            c = (Resource) db.asValue(c);
//
//            final long _s = assertTerm(s);
//            final long _p = assertTerm(p);
//            final long _o = assertTerm(o);
//            final long _c = (c == null ? NULL : assertTerm(c)); // FIXME consider context in asserts.

            
            
            /*
             * Now verify the statements using the assigned term identifiers.
             * 
             * @todo not handling the context position - it is either unbound or
             * a statement identifier.
             */
            
//            if (_s != NULL && _p != NULL && _o != NULL) {
//
//                assertStatement(db, SPOKeyOrder.SPO, _s, _p, _o, stmt);
//
//                assertStatement(db, SPOKeyOrder.OSP, _s, _p, _o, stmt);
//
//                assertStatement(db, SPOKeyOrder.POS, _s, _p, _o, stmt);
//
//            }

            {

                final SPO[] b = new SPO[n];

                int n2 = 0;
                for(Statement stmt : a) {
                    
                    final BigdataResource s = (BigdataResource) db.asValue(termSet.get(stmt.getSubject()));
                    
                    final BigdataURI p = (BigdataURI) db.asValue(termSet.get(stmt.getPredicate()));
                    
                    final BigdataValue o = (BigdataValue) db.asValue(termSet.get(stmt.getObject()));
                    
                    boolean ok = true;
                    if(s == null) {
                        
                        log.error("Subject not found: "+stmt.getSubject());
                        ok = false;
                        
                    }
                    if(p == null) {
                        
                        log.error("Predicate not found: "+stmt.getPredicate());
                        ok = false;
                        
                    }
                    if(o == null) {
                        
                        log.error("Object not found: "+stmt.getObject());
                        ok = false;

                    }
                 
                    if(!ok) {
                        
                        log.error("Unable to resolve statement with unresolvable terms: "+stmt);
                        
                    } else {

                        // Leave the StatementType blank for bulk complete.
                        b[n2++] = new SPO(s.getTermId(), p.getTermId(), o
                                .getTermId() /*, StatementType */);

                    }
                    
                }

                final IChunkedOrderedIterator<SPO> itr = db
                        .bulkCompleteStatements(b, n2);

                try {

                    while (itr.hasNext()) {

                        final SPO spo = itr.next();

                        if (!spo.hasStatementType()) {

                            error("Statement not found: " + spo.toString(db));

                        }

                    }

                } finally {

                    itr.close();

                }
                
            }

        }
        
// /**
// * Verify the {@link Value} is in both the forward and
// * reverse indices.
// *
//         * @param v
//         *            The value.
//         * 
//         * @return the term identifier for that value -or-
//         *         <code>null</code> if it was not propertly
//         *         found in both the forward and reverse
//         *         indices.
//         */
//        private long assertTerm(Value v) {
//
//            boolean ok = true;
//
//            // lookup in the term:id index.
//            long termId = db.getTermId(v);
//
//            if (termId == NULL) {
//
//                log.error("Term not in forward index: " + v);
//
//                nerrs.incrementAndGet();
//
//                ok = false;
//
//            } else {
//
//                // lookup in the id:term index.
//                Value v2 = db.asValue(db.getTerm(termId));
//
//                if (v2 == null) {
//
//                    log.error("Term not in reverse index: " + v);
//
//                    nerrs.incrementAndGet();
//
//                    ok = false;
//
//                    //                            } else if(v.compareTo(v2)!=0) {
//                } else if (!v.equals(v2)) {
//
//                    log.error("Wrong term in reverse index: expected="
//                            + v + ", but actual=" + v2);
//
//                    nerrs.incrementAndGet();
//
//                    ok = false;
//
//                }
//
//            }
//
//            return ok ? termId : NULL;
//
//        }
//
//        /**
//         * Verify that statements are found in the specified statement indices.
//         * 
//         * @param db
//         * @param keyOrder
//         * @param n
//         * @param a
//         */
//        private void assertStatement(AbstractTripleStore db,
//                IKeyOrder<SPO> keyOrder, long s, long p, long o,
//                Statement stmt) {
//
//            final IIndex ndx = db.getStatementIndex(keyOrder);
//
//            final SPOTupleSerializer tupleSer = (SPOTupleSerializer) ndx
//                    .getIndexMetadata().getTupleSerializer();
//
//            final SPO expectedSPO = new SPO(s, p, o,
//                    StatementEnum.Explicit);
//
//            final byte[] fromKey = tupleSer.statement2Key(keyOrder,
//                    expectedSPO);
//
//            final byte[] toKey = null;
//
//            final ITupleIterator itr = ndx
//                    .rangeIterator(fromKey, toKey);
//
//            if (!itr.hasNext()) {
//
//                /*
//                 * This happens when the statement is not in the
//                 * index AND there is no successor of the
//                 * statement in the index.
//                 */
//
//                log.error("Statement not found" + ": index=" + keyOrder
//                        + ", stmt=" + stmt + expectedSPO);
//
//                nerrs.incrementAndGet();
//
//                return;
//
//            }
//
//            final SPO actualSPO = (SPO) itr.next().getObject();
//
//            if (!expectedSPO.equals(actualSPO)) {
//
//                /*
//                 * This happens when the statement is not in the
//                 * index but there is a successor of the
//                 * statement in the index.
//                 */
//                log.error("Statement not found" + ": index=" + keyOrder
//                        + ", stmt=" + stmt + ", expected="
//                        + expectedSPO + ", nextInIndexOrder="
//                        + actualSPO);
//
//                nerrs.incrementAndGet();
//
//            }
//
//        }

    }

}
