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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.LocalTripleStoreWithEmbeddedDataService;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;
import com.bigdata.rdf.util.KeyOrder;

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

    /** @todo use some RDF/XML that we can pack with the distribution. */
    public void test_loadAndVerify() throws Exception {
        
//      String file = "../rdf-data/nciOncology.owl";
        String file = "../rdf-data/alibaba_v41.rdf";
        
        if(!new File(file).exists()) {
            
            log.warn("Resource not found: "+file+", test skipped: "+getName());
            
            return;
            
        }

        AbstractTripleStore store = getStore();
        
        if(store instanceof ScaleOutTripleStore || store instanceof LocalTripleStoreWithEmbeddedDataService) {

            /*
             * FIXME The test needs to use the batch API to verify the
             * statements in the scale-out triple store.
             */
            fail("Test needs batch verification method for data service versions.");
            
        }
        
        try {
    
        // avoid modification of the properties.
        Properties properties = new Properties(getProperties());
        
        // turn off RDFS closure for this test.
        properties.setProperty(DataLoader.Options.CLOSURE, ClosureEnum.None.toString());
        
        DataLoader dataLoader = new DataLoader(properties,store);
        
        // load into the datbase.
        dataLoader.loadData(file, "" /* baseURI */, RDFFormat.RDFXML);
        
        store.commit();
        
        if(store.isStable()) {
            
            store = reopenStore(store);
            
        }

        store.predicateUsage();
        
        /*
         * re-parse and verify all statements exist in the db using each
         * statement index.
         */
        final AtomicInteger nerrs = new AtomicInteger(0);
        {
            
            final AbstractTripleStore db = store;
            
            BasicRioLoader loader = new BasicRioLoader() {

                public RDFHandler newRDFHandler() {

                    return new RDFHandlerBase() {

                        public void handleStatement(Statement stmt) {

                            if (nerrs.get() > 20) {

                                throw new RuntimeException("Too many errors");
                                
                            }
                            
                            Resource s = stmt.getSubject();
                            URI p = stmt.getPredicate();
                            Value o = stmt.getObject();
                            Resource c = stmt.getContext();
                            
                            // convert to Sesame objects.
                            s = (Resource) db.asValue(s);
                            p = (URI) db.asValue(p);
                            o = (Value) db.asValue(o);
                            c = (Resource) db.asValue(c);
                            
                            final long _s = assertTerm(s);
                            final long _p = assertTerm(p);
                            final long _o = assertTerm(o);
                            final long _c = (c==null?NULL:assertTerm(c)); // FIXME consider context in asserts.
                            
                            if (_s != NULL && _p != NULL && _o != NULL) {
                            
                                assertStatement(db, KeyOrder.SPO, _s, _p, _o,
                                        stmt );
                                
                                assertStatement(db, KeyOrder.OSP, _s, _p, _o,
                                        stmt );

                                assertStatement(db, KeyOrder.POS, _s, _p, _o,
                                        stmt );

                            }
                            
                        }

                        /**
                         * Verify the {@link Value} is in both the forward and
                         * reverse indices.
                         * 
                         * @param v
                         *            The value.
                         * 
                         * @return the term identifier for that value -or-
                         *         <code>null</code> if it was not propertly
                         *         found in both the forward and reverse
                         *         indices.
                         */
                        private long assertTerm(Value v) {

                            boolean ok = true;

                            // lookup in the term:id index.
                            long termId = db.getTermId(v);

                            if (termId == NULL) {
                            
                                log.error("Term not in forward index: "+v);
                                
                                nerrs.incrementAndGet();
                                
                                ok = false;                                

                            } else {

                                // lookup in the id:term index.
                                Value v2 = db.asValue(db.getTerm(termId));
                                
                                if(v2 == null) {

                                    log.error("Term not in reverse index: "+v);

                                    nerrs.incrementAndGet();
                                    
                                    ok = false;
                                    
//                                } else if(v.compareTo(v2)!=0) {
                                } else if(!v.equals(v2)) {

                                    log.error("Wrong term in reverse index: expected="+v+", but actual="+v2);

                                    nerrs.incrementAndGet();
                                    
                                    ok = false;

                                }

                            }
                            
                            return ok?termId:NULL;

                        }
                        
                        /**
                         * Verify that the statement is found in each of the
                         * statement indices.
                         * 
                         * @param db
                         * @param keyOrder
                         * @param s
                         * @param p
                         * @param o
                         * @param stmt
                         */
                        private void assertStatement(
                                AbstractTripleStore db, KeyOrder keyOrder,
                                long s, long p, long o,
                                Statement stmt) {

                            IIndex ndx = db.getStatementIndex(keyOrder);

                            final SPO expectedSPO = new SPO(s, p, o,
                                    StatementEnum.Explicit);
                            
                            final byte[] fromKey = db.getKeyBuilder()
                                        .statement2Key(keyOrder, expectedSPO);

                            final byte[] toKey = null;

                            IEntryIterator itr = ndx.rangeIterator(fromKey,
                                    toKey);

                            if (!itr.hasNext()) {

                                /*
                                 * This happens when the statement is not in the
                                 * index AND there is no successor of the
                                 * statement in the index.
                                 */
                                
                                log.error("Statement not found"
                                            + ": index=" + keyOrder + ", stmt="
                                            + stmt + expectedSPO);
                            
                                nerrs.incrementAndGet();

                                return;
                                
                            }

                            final SPO actualSPO = new SPO(keyOrder, itr);

                            if (!expectedSPO.equals(actualSPO)) {

                                /*
                                 * This happens when the statement is not in the
                                 * index but there is a successor of the
                                 * statement in the index.
                                 */
                                log.error("Statement not found"
                                                + ": index=" + keyOrder
                                                + ", stmt=" + stmt
                                                + ", expected="
                                                + expectedSPO
                                                + ", nextInIndexOrder="
                                                + actualSPO);
                                
                                nerrs.incrementAndGet();
                                
                            }
                        
                        }                     

                    };

                }

            };
            
            loader.loadRdf(new BufferedReader(new InputStreamReader(
                    new FileInputStream(file))), ""/* baseURI */,
                    RDFFormat.RDFXML, false/* verify */);
            
        }
        
        assertEquals("nerrors",0,nerrs.get());

        assertStatementIndicesConsistent(store);
        
        } finally {
        
            store.closeAndDelete();
            
        }
        
    }

}
