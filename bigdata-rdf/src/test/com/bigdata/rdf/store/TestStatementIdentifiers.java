/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Apr 10, 2008
 */

package com.bigdata.rdf.store;

import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.openrdf.model.Statement;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.rdfxml.RDFXMLWriter;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rio.StatementCyclesException;
import com.bigdata.rdf.rio.UnificationException;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOComparator;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Test of the statement identifier semantics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestStatementIdentifiers extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestStatementIdentifiers() {
    }

    /**
     * @param name
     */
    public TestStatementIdentifiers(String name) {
        super(name);
    }
    
    /**
     * Some validation of the logic for assigning statement identifiers to
     * explicit statements.
     */
    public void test_statementIdentifiers() {

        final Properties properties = super.getProperties();

        // override the default axiom model.
        properties.setProperty(
                com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());

        AbstractTripleStore store = getStore(properties);

        try {
            
            if (!store.isStatementIdentifiers()) {
                
                log.warn("Statement identifiers are not enabled");
                
                return;
                
            }
            
            final BigdataValueFactory f = store.getValueFactory();

            final BigdataURI x = f.createURI("http://www.foo.org/x");
            final BigdataURI y = f.createURI("http://www.foo.org/y");
            final BigdataURI z = f.createURI("http://www.foo.org/z");
    
            final BigdataURI A = f.createURI("http://www.foo.org/A");
            final BigdataURI B = f.createURI("http://www.foo.org/B");
            final BigdataURI C = f.createURI("http://www.foo.org/C");
    
            final BigdataURI rdfType = f.asValue(RDF.TYPE);
            final BigdataURI rdfsLabel = f.asValue(RDFS.LABEL);
            final BigdataURI rdfsSubClassOf = f.asValue(RDFS.SUBCLASSOF);
    
            final BigdataLiteral lit1 = f.createLiteral("abc");
            final BigdataLiteral lit2 = f.createLiteral("abc", A);
            final BigdataLiteral lit3 = f.createLiteral("abc", "en");
    
            final BigdataBNode bn1 = f.createBNode(UUID.randomUUID().toString());
            final BigdataBNode bn2 = f.createBNode("a12");
            
            {
                final BigdataValue[] terms = new BigdataValue[] {

                        x, y, z,//
                        A, B, C,//
                        rdfType,//
                        rdfsLabel,//
                        rdfsSubClassOf,//
                        lit1, lit2, lit3,//
                        bn1, bn2 //

                };

                store.addTerms(terms);
                
            }
            
            final SPO[] stmts = new SPO[] {

                new SPO(x.getIV(), rdfType.getIV(), C.getIV(), StatementEnum.Explicit),
                new SPO(y.getIV(), rdfType.getIV(), B.getIV(), StatementEnum.Explicit),
                new SPO(z.getIV(), rdfType.getIV(), A.getIV(), StatementEnum.Explicit),
                
                new SPO(A.getIV(), rdfsLabel.getIV(), lit1.getIV(), StatementEnum.Explicit),
                new SPO(B.getIV(), rdfsLabel.getIV(), lit2.getIV(), StatementEnum.Explicit),
                new SPO(C.getIV(), rdfsLabel.getIV(), lit3.getIV(), StatementEnum.Explicit),
                
                new SPO(B.getIV(), rdfsSubClassOf.getIV(), A.getIV(), StatementEnum.Explicit),
                new SPO(C.getIV(), rdfsSubClassOf.getIV(), B.getIV(), StatementEnum.Explicit),
                    
                new SPO(bn1.getIV(), rdfsLabel.getIV(), lit1.getIV(), StatementEnum.Explicit),
                new SPO(bn2.getIV(), rdfsLabel.getIV(), lit2.getIV(), StatementEnum.Explicit),

            };

            /*
             * Add the statements to the KB.
             * 
             * Note: sorts into SPO order as a side-effect.
             */
            store.addStatements(stmts, stmts.length);
            
            /*
             * Verify statement identifiers were assigned.
             */
            for(int i=0; i<stmts.length; i++) {
                
                final long sid = stmts[i].getStatementIdentifier();
                
                assertNotSame(NULL, sid);
                
                assertTrue(AbstractTripleStore.isStatement(sid));
                assertFalse(AbstractTripleStore.isLiteral(sid));
                assertFalse(AbstractTripleStore.isURI(sid));
                assertFalse(AbstractTripleStore.isBNode(sid));
                
                if (log.isInfoEnabled())
                    log.info(stmts[i].toString(store) + " ::: "
                            + stmts[i].toString());
                
            }
            
            /*
             * Verify read back from the KB. Note that the SPO[] is already in
             * SPO order and we are reading in SPO order so the array and the
             * iterator should be aligned and should visit the same #of
             * statements (since there is no truth maintenance in this test
             * there are no entailments and a closed world assumption is Ok).
             */
            {

                final IChunkedOrderedIterator<ISPO> itr = store.getAccessPath(SPOKeyOrder.SPO).iterator();
                
                try {
                
                for(int i=0; i<stmts.length; i++) {
                    
                    assertTrue("expecting more elements: i="+i, itr.hasNext());
                    
                    final SPO expected = stmts[i];
                    
                    final ISPO actual = itr.next();

                    assertEquals("S @ i=" + i, expected.s, actual.s());

                    assertEquals("P @ i=" + i, expected.p, actual.p());

                    assertEquals("O @ i=" + i, expected.o, actual.o());

                    assertEquals("TYPE @ i=" + i, expected.getStatementType(), actual.getStatementType());

                    assertEquals("SID @ i=" + i, expected
                            .getStatementIdentifier(), actual
                            .getStatementIdentifier());

                }
                
                } finally {
                    
                    itr.close();
                    
                }

            }

            /*
             * Verify same statement identifiers assigned if we re-assert the
             * statements.
             * 
             * Note: this creates a new SPO[] with the same {s,p,o}:type data
             * but without the statement identifiers. The array is in the same
             * order (SPO order) as the original SPO[]. We then insert the new
             * SPO[] into the KB and compare the assigned statement identifiers
             * with the ones that were assigned above.
             */
            {

                SPO[] a = new SPO[stmts.length];
                
                for(int i=0; i<stmts.length; i++) {
                    
                    SPO spo = stmts[i];
                    
                    a[i] = new SPO(spo.s, spo.p, spo.o, spo.getStatementType());
                    
                }
                
                final long nwritten = store.addStatements(a, a.length);

                for(int i=0;i<stmts.length; i++) {
                    
                    assertEquals("sid @ i=" + i, stmts[i]
                            .getStatementIdentifier(), a[i]
                            .getStatementIdentifier());
                    
                }
                
                /*
                 * Note: no statements should have been written on the kb since
                 * they were all pre-existing. addStatements() should have just
                 * read the pre-existing statement identifiers out of the KB and
                 * assigned them to a[].
                 */
                assertEquals("#written", 0L, nwritten);
                
            }

            final BigdataStatementIterator itr = store.getStatements(null, null, null);
            
            try {

                Writer w = new StringWriter();
                
                RDFXMLWriter rdfWriter = new RDFXMLWriter(w);
                
                rdfWriter.startRDF();

                while(itr.hasNext()) {
                
                    Statement stmt = itr.next();
                    
                    rdfWriter.handleStatement(stmt);
                    
                }
                
                rdfWriter.endRDF();
                
                if (log.isInfoEnabled())
                    log.info(w.toString());
                
            } catch(Exception ex) {
                
                throw new RuntimeException(ex);
                
            } finally {
                
                itr.close();
                
            }
            
            /*
             * Verify after restart.
             */
            if (store.isStable()) {

                // flush any changes.
                store.commit();

                // re-open the database.
                store = reopenStore(store);

                /*
                 * Verify read back from the KB. Note that the SPO[] is already
                 * in SPO order and we are reading in SPO order so the array and
                 * the iterator should be aligned and should visit the same #of
                 * statements (since there is no truth maintenance in this test
                 * there are no entailments and a closed world assumption is
                 * Ok).
                 */
                assertSameSPOArray(store, stmts, stmts.length);

                /*
                 * Verify same statement identifiers assigned if we re-assert
                 * the statements.
                 * 
                 * Note: this creates a new SPO[] with the same {s,p,o}:type
                 * data but without the statement identifiers. The array is in
                 * the same order (SPO order) as the original SPO[]. We then
                 * insert the new SPO[] into the KB and compare the assigned
                 * statement identifiers with the ones that were assigned above.
                 */
                {

                    SPO[] a = new SPO[stmts.length];

                    for (int i = 0; i < stmts.length; i++) {

                        SPO spo = stmts[i];

                        a[i] = new SPO(spo.s, spo.p, spo.o, spo.getStatementType());

                    }

                    final long nwritten = store.addStatements(a, a.length);

                    for (int i = 0; i < stmts.length; i++) {

                        assertEquals("sid @ i=" + i, stmts[i]
                                .getStatementIdentifier(), a[i]
                                .getStatementIdentifier());

                    }

                    /*
                     * Note: no statements should have been written on the kb
                     * since they were all pre-existing. addStatements() should
                     * have just read the pre-existing statement identifiers out
                     * of the KB and assigned them to a[].
                     */
                    assertEquals("#written", 0L, nwritten);

                }
            }

        } catch(Exception ex) {
            
            log.error(ex.getMessage(),ex);
            
        } finally {

            store.__tearDownUnitTest();

        }

    }

    /**
     * Test creates a statement (a) and then a statement (b) about statement (a)
     * using the statement identifier for (a) and then retracts (a) and verifies
     * that (b) is also retracted.
     */
    public void test_retractionOfStatementsAboutStatements() {
        
        final Properties properties = super.getProperties();
        
        // override the default axiom model.
        properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        final AbstractTripleStore store = getStore(properties);
        
        try {

            if(!store.isStatementIdentifiers()) {
                
                log.warn("Statement identifiers are not enabled");

                return;
                
            }

            final BigdataValueFactory f = store.getValueFactory();

            final BigdataURI x = f.createURI("http://www.foo.org/x");
            final BigdataURI y = f.createURI("http://www.foo.org/y");
            final BigdataURI z = f.createURI("http://www.foo.org/z");
    
            final BigdataURI A = f.createURI("http://www.foo.org/A");
            final BigdataURI B = f.createURI("http://www.foo.org/B");
            final BigdataURI C = f.createURI("http://www.foo.org/C");
    
            final BigdataURI rdfType = f.asValue(RDF.TYPE);
//            final BigdataURI rdfsLabel = f.createURI(RDFS.LABEL);
            final BigdataURI dcCreator = f.createURI("http://purl.org/dc/terms/creator");
    
            BigdataLiteral lit1 = f.createLiteral("bryan");
            BigdataLiteral lit2 = f.createLiteral("mike");
    
//            final BigdataBNode bn1 = f.createBNode(UUID.randomUUID().toString());
//            final BigdataBNode bn2 = f.createBNode("a12");

            {
                final BigdataValue[] terms = new BigdataValue[] {

                x, y, z,//
                        A, B, C,//
                        rdfType,//
                        // rdfsLabel,//
                        dcCreator,//
                        lit1, lit2,//
                        // bn1,bn2//

                };

                store.addTerms(terms);
            }
            
            final SPO[] stmts1 = new SPO[] {

                new SPO(x.getIV(), rdfType.getIV(), A.getIV(), StatementEnum.Explicit),
                
            };
            
            assertEquals(1,store.addStatements(stmts1, stmts1.length));

            final long sid1 = stmts1[0].getStatementIdentifier();
            
            final SPO[] stmts2 = new SPO[] {
              
                    new SPO(sid1, dcCreator.getIV(), lit1.getIV(), StatementEnum.Explicit),
                    
            };

            assertEquals(1,store.addStatements(stmts2, stmts2.length));

            assertEquals(2,store.getStatementCount(true/*exact*/));

            /*
             * Verify read back.
             */
            {
            
                SPO[] all = new SPO[] {
                  
                        stmts1[0],
                        stmts2[0]
                        
                };
            
                assertSameSPOArray(store, all, all.length);
                            
            }

            /*
             * Retract the original statement and verify that the statement
             * about that statement is also retracted.
             */
            {
                
                store.removeStatements(new ChunkedArrayIterator<ISPO>(
                        stmts1.length, stmts1, null/*keyOrder*/));

                assertSameSPOArray(store, new SPO[]{}, 0/*numStmts*/);

            }
            
        } finally {

            store.__tearDownUnitTest();

        }
        
    }
    
    /**
     * Test creates a statement (a), a statement (b) about statement (a), and a
     * statement (c) about statement (b) and then retracts (a) and verifies that
     * both (b) and (c) are also retracted.
     */
    public void test_retractionOfStatementsAboutStatements2() {
        
        final Properties properties = super.getProperties();
        
        // override the default axiom model.
        properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        final AbstractTripleStore store = getStore(properties);
        
        try {

            if(!store.isStatementIdentifiers()) {
                
                log.warn("Statement identifiers are not enabled");

                return;
                
            }

            final BigdataValueFactory f = store.getValueFactory();

            final BigdataURI x = f.createURI("http://www.foo.org/x");
            final BigdataURI y = f.createURI("http://www.foo.org/y");
            final BigdataURI z = f.createURI("http://www.foo.org/z");
    
            final BigdataURI A = f.createURI("http://www.foo.org/A");
            final BigdataURI B = f.createURI("http://www.foo.org/B");
            final BigdataURI C = f.createURI("http://www.foo.org/C");
    
            final BigdataURI rdfType = f.asValue(RDF.TYPE);
            final BigdataURI dcCreator = f.createURI("http://purl.org/dc/terms/creator");
    
            final BigdataLiteral lit1 = f.createLiteral("bryan");
            final BigdataLiteral lit2 = f.createLiteral("mike");

            {

                final BigdataValue[] terms = new BigdataValue[] {

                        x, y, z,//
                        A, B, C,//
                        rdfType,//
                        dcCreator,//
                        lit1, lit2,//

                };

                store.addTerms(terms);

            }
            
            /*
             * create the original statement.
             */
            SPO[] stmts1 = new SPO[] {

                new SPO(x.getIV(), rdfType.getIV(), A.getIV(), StatementEnum.Explicit),
                
            };
            
            // insert into the database.
            assertEquals(1,store.addStatements(stmts1, stmts1.length));
            
            // the statement identifier for the original stmt. 
            final long sid1 = stmts1[0].getStatementIdentifier();
            
            /*
             * create a statement about that statement.
             */
            
            SPO[] stmts2 = new SPO[] {
                    
                    new SPO(sid1, dcCreator.getIV(), lit1.getIV(), StatementEnum.Explicit),
                    
            };

            // insert the metadata statement into the database.
            assertEquals(1,store.addStatements(stmts2, stmts2.length));

            assertEquals(2,store.getStatementCount(true/*exact*/));

            // the stmt identifier for the statement about the original stmt.
            final long sid2 = stmts2[0].getStatementIdentifier();

            /*
             * create a statement about the statement about the original statement.
             */

            SPO[] stmts3 = new SPO[] {
                    
                    new SPO(sid2, dcCreator.getIV(), lit2.getIV(),
                    StatementEnum.Explicit),
                    
            };

            // insert the metadata statement into the database.
            assertEquals(1,store.addStatements(stmts3, stmts3.length));

            assertEquals(3,store.getStatementCount(true/*exact*/));

            /*
             * Verify read back.
             */
            {
            
                SPO[] all = new SPO[] {
                  
                        stmts1[0],
                        stmts2[0],
                        stmts3[0],
                        
                };
                
                assertSameSPOArray(store, all, all.length);
                            
            }

            /*
             * Retract the original statement and verify that the statement
             * about the original statement and the statement about the
             * statement about the original statement are also retracted.
             */
            {
                
                store.removeStatements(new ChunkedArrayIterator<ISPO>(
                        stmts1.length, stmts1, null/*keyOrder*/));

                assertSameSPOArray(store, new SPO[]{}, 0/*numStmts*/);

            }
            
        } finally {

            store.__tearDownUnitTest();

        }
        
    }
    
    /**
     * Verify read back from the KB. Note that the SPO[] MUST be in SPO order
     * and we are reading in SPO order so the array and the iterator should be
     * aligned and should visit the same #of statements (since there is no truth
     * maintenance in this test there are no entailments and a closed world
     * assumption is Ok).
     * 
     * @param store
     * @param all
     *            The expected statemetns (sorted into SPO order as a
     *            side-effect).
     * @param numStmts
     *            The #of statements <i>all</i>.
     */
    private void assertSameSPOArray(AbstractTripleStore store, SPO[] all, int numStmts) {
        
        Arrays.sort(all, 0, all.length, SPOComparator.INSTANCE);

        final IChunkedOrderedIterator<ISPO> itr = store.getAccessPath(
                SPOKeyOrder.SPO).iterator();

        try {

            for (int i = 0; i < all.length; i++) {

                assertTrue("i=" + i, itr.hasNext());

                final SPO expected = all[i];

                final ISPO actual = itr.next();

                assertEquals("S @ i=" + i, expected.s, actual.s());

                assertEquals("P @ i=" + i, expected.p, actual.p());

                assertEquals("O @ i=" + i, expected.o, actual.o());

                assertEquals("TYPE @ i=" + i, expected.getStatementType(), actual
                        .getStatementType());

                assertEquals("SID @ i=" + i, expected.getStatementIdentifier(),
                        actual.getStatementIdentifier());

            }

            assertFalse("iterator is willing to visit more than " + numStmts
                    + " statements", itr.hasNext());
        } finally {

            itr.close();

        }
        
    }

    /**
     * Tests the correct rejection of a statement about itself.
     */
    public void test_correctRejection_cycles01() {
        
        AbstractTripleStore store = getStore();

        try {

            if (!store.isStatementIdentifiers()) {

                log.warn("Statement identifiers are not enabled");

                return;

            }

            final BigdataValueFactory valueFactory = store.getValueFactory();
            final BigdataURI A = valueFactory.createURI("http://www.foo.org/A");
            final BigdataURI rdfType = valueFactory.createURI(RDF.TYPE.stringValue());
            final BigdataBNode sid1 = valueFactory.createBNode("_S1");

            /*
             * Note: Do NOT add the SIDs in advance to the lexicon. The will be
             * assigned blank node term identifiers rather than statement term
             * identifiers (SIDs). These differ in their bit pattern on the
             * lower two bits.
             */
//            store.addTerms(new BigdataValue[] { A, rdfType, sid1 });
            
            {
                StatementBuffer buf = new StatementBuffer(store, 100/* capacity */);

                // statement about itself is a cycle.
                buf.add(sid1, rdfType, A, sid1);

                /*
                 * Flush to the database, resolving statement identifiers as
                 * necessary.
                 */
                try {

                    buf.flush();

                    fail("Expecting: " + StatementCyclesException.class);

                } catch (StatementCyclesException ex) {

                    System.err.println("Ignoring expected exception: " + ex);

                }

            }
            
        } finally {
            
            store.__tearDownUnitTest();
            
        }
        
    }
    
    /**
     * Tests the correct rejection of cycles within statements about statements.
     */
    public void test_correctRejection_cycles02() {
        
        AbstractTripleStore store = getStore();

        try {

            if (!store.isStatementIdentifiers()) {

                log.warn("Statement identifiers are not enabled");

                return;

            }

            final BigdataValueFactory valueFactory = store.getValueFactory();
            final BigdataURI B = valueFactory.createURI("http://www.foo.org/B");
            final BigdataURI rdfType = valueFactory.createURI(RDF.TYPE.stringValue());
            final BigdataBNode sid1 = valueFactory.createBNode("_S1");
            final BigdataBNode sid2 = valueFactory.createBNode("_S2");

            /*
             * Note: Do NOT add the SIDs in advance to the lexicon. The will be
             * assigned blank node term identifiers rather than statement term
             * identifiers (SIDs). These differ in their bit pattern on the
             * lower two bits.
             */
//            store.addTerms(new BigdataValue[] { B, rdfType, sid1, sid2 });
            
            {
                StatementBuffer buf = new StatementBuffer(store, 100/* capacity */);

                // a cycle with a period of one.
                buf.add(sid2, rdfType, B, sid1);
                buf.add(sid1, rdfType, B, sid2);
                
                /*
                 * Flush to the database, resolving statement identifiers as
                 * necessary.
                 */
                try {

                    buf.flush();

                    fail("Expecting: "+StatementCyclesException.class);
                    
                } catch(StatementCyclesException ex) {
                    
                    System.err.println("Ignoring expected exception: "+ex);
                    
                }

            }
            
        } finally {
            
            store.__tearDownUnitTest();
            
        }
        
    }
    
    /**
     * Tests the correct rejection when the same blank node is used in the
     * context position of more than one statement.
     */
    public void test_correctRejection_unificationError() {
        
        AbstractTripleStore store = getStore();

        try {

            if (!store.isStatementIdentifiers()) {

                log.warn("Statement identifiers are not enabled");

                return;

            }

            final BigdataValueFactory f = store.getValueFactory();

            final BigdataURI A = f.createURI("http://www.foo.org/A");
            final BigdataURI B = f.createURI("http://www.foo.org/B");
            final BigdataURI C = f.createURI("http://www.foo.org/C");
            final BigdataURI rdfType = f.asValue(RDF.TYPE);
            final BigdataBNode sid1 = f.createBNode("_S1");

            StatementBuffer buf = new StatementBuffer(store, 100/* capacity */);
            
            // same blank node in both two distinct statement is an error.
            buf.add(A, rdfType, C, sid1);
            buf.add(B, rdfType, C, sid1);
            
            /*
             * Flush to the database, resolving statement identifiers as
             * necessary.
             */
            try {

                buf.flush();

                fail("Expecting: "+UnificationException.class);
                
            } catch(UnificationException ex) {
                
                System.err.println("Ignoring expected exception: "+ex);
                
            }

        } finally {
            
            store.__tearDownUnitTest();
            
        }
        
    }
    
}
