/*

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
 * Created on Jan 27, 2007
 */

package com.bigdata.rdf.store;

import java.util.UUID;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.lexicon.Id2TermWriteProc;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.lexicon.Term2IdWriteProc;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.striterator.ChunkedArrayIterator;

/**
 * Test basic features of the {@link ITripleStore} API.
 * 
 * @todo Add tests of race condition where two threads attempt to: (1) add the
 *       same term(s); (2) resolve the same term(s). While the {@link Term2IdWriteProc}
 *       and {@link Id2TermWriteProc} operations are designed to avoid the race condition
 *       in the indices using consistent unisolated operations, however the
 *       termCache can also have race conditions where one thread "wins" and
 *       inserts its definition for a term and the other thread "loses" and an
 *       IllegalStateException is thrown by the cache when it tries to insert
 *       its own definition (distinct object) for the same term.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTripleStore extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestTripleStore() {
    }

    /**
     * @param name
     */
    public TestTripleStore(String name) {
        super(name);
    }

    /**
     * Test helper verifies that the term is not in the lexicon, adds the term
     * to the lexicon, verifies that the term can be looked up by its assigned
     * term identifier, verifies that the term is now in the lexicon, and
     * verifies that adding the term again returns the same term identifier.
     * 
     * @param term
     *            The term.
     */
    protected void doAddTermTest(AbstractTripleStore store, Value term) {

        assertEquals(NULL, store.getTermId(term));

        // add to the database.
        final long id = store.addTerm(term);

        // verify that we did not assign NULL (0L).
        assertNotSame(NULL, id);

        if(term instanceof Literal) {
         
            assertTrue("isLiteral()",AbstractTripleStore.isLiteral(id));
            assertFalse("isBNode()",AbstractTripleStore.isBNode(id));
            assertFalse("isURI()",AbstractTripleStore.isURI(id));
            assertFalse("isStatement()",AbstractTripleStore.isStatement(id));
            
        } else if( term instanceof BNode) {

            assertFalse("isLiteral()",AbstractTripleStore.isLiteral(id));
            assertTrue("isBNode()",AbstractTripleStore.isBNode(id));
            assertFalse("isURI()",AbstractTripleStore.isURI(id));
            assertFalse("isStatement()",AbstractTripleStore.isStatement(id));

        } else if( term instanceof URI) {

            assertFalse("isLiteral()",AbstractTripleStore.isLiteral(id));
            assertFalse("isBNode()",AbstractTripleStore.isBNode(id));
            assertTrue("isURI()",AbstractTripleStore.isURI(id));
            assertFalse("isStatement()",AbstractTripleStore.isStatement(id));

        }

        if (store.getLexiconRelation().isStoreBlankNodes()
                || !(term instanceof BNode)) {

            // test lookup in the forward mapping (term -> id)
            assertEquals("forward mapping", id, store.getTermId(term));

        }

        // blank nodes are NEVER stored in the reverse index.
        if (!(term instanceof BNode)) {

            // test lookup in the reverse mapping (id -> term)
            assertEquals("reverse mapping", term, store.getTerm(id));

        }

        if (store.getLexiconRelation().isStoreBlankNodes()
                || !(term instanceof BNode)) {

            // re-assert the term and verify that the same id is assigned.
            assertEquals("add is not stable?", id, store.addTerm(term));
            
        }

    }

    /**
     * Simple test of inserting one term at a time into the lexicon.
     */
    public void test_addTerm() {

        AbstractTripleStore store = getStore();
        
        try {

            doAddTermTest(store, new LiteralImpl("abc"));
            doAddTermTest(store, new LiteralImpl("abc", XMLSchema.DECIMAL));
            doAddTermTest(store, new LiteralImpl("abc", "en"));
    
            doAddTermTest(store, new URIImpl("http://www.bigdata.com"));
            doAddTermTest(store, RDF.TYPE);
            doAddTermTest(store, RDFS.SUBCLASSOF);
            doAddTermTest(store, XMLSchema.DECIMAL);
    
            doAddTermTest(store, new BNodeImpl(UUID.randomUUID().toString()));
            doAddTermTest(store, new BNodeImpl("a12"));
    
//            store.commit();
    
            dumpTerms(store);

            /*
             * verify that we can detect literals by examining the term identifier.
             */

            assertTrue(AbstractTripleStore.isLiteral(store.getTermId(new LiteralImpl("abc"))));
            
            assertTrue(AbstractTripleStore.isLiteral(store.getTermId(new LiteralImpl("abc",XMLSchema.DECIMAL))));
            
            assertTrue(AbstractTripleStore.isLiteral(store.getTermId(new LiteralImpl("abc", "en"))));

            assertFalse(AbstractTripleStore.isLiteral(store.getTermId(new URIImpl("http://www.bigdata.com"))));

            assertFalse(AbstractTripleStore.isLiteral(store.getTermId(RDF.TYPE)));

            assertFalse(AbstractTripleStore.isLiteral(store.getTermId(new BNodeImpl(UUID.randomUUID().toString()))));
            
            assertFalse(AbstractTripleStore.isLiteral(store.getTermId(new BNodeImpl("a12"))));
            
        } finally {
            
            store.closeAndDelete();

        }

    }

    /**
     * Simple test of batch insertion of terms into the lexicon.
     */
    public void test_insertTerms() {

        final AbstractTripleStore store = getStore();

        try {
        
            final BigdataValueFactory valueFactory = store.getValueFactory();
            
            final BigdataValue[] terms = new BigdataValue[] {//
    
                valueFactory.createURI("http://www.bigdata.com"),//

                valueFactory.createURI(RDF.TYPE.stringValue()),//
                valueFactory.createURI(RDFS.SUBCLASSOF.stringValue()),//
                valueFactory.createURI(XMLSchema.DECIMAL.stringValue()),//

                valueFactory.createLiteral("abc"),//
                valueFactory.createLiteral("abc", XMLSchema.DECIMAL),//
                valueFactory.createLiteral("abc", "en"),//

                valueFactory.createBNode(UUID.randomUUID().toString()),//
                valueFactory.createBNode("a12") //

            };
    
            store.addTerms(terms);
    
            final boolean storeBlankNodes = store.getLexiconRelation().isStoreBlankNodes();
            
            for (int i = 0; i < terms.length; i++) {
    
                // verify set by side-effect on batch insert.
                assertNotSame(NULL, terms[i].getTermId());
                
                // save & clear
                final long termId = terms[i].getTermId();
                terms[i].clearTermId();

                if (storeBlankNodes || !(terms[i] instanceof BNode)) {

                    // check the forward mapping (term -> id)
                    assertEquals("forward mapping", termId, store
                            .getTermId(terms[i]));
                    
                    // verify set by side effect.
                    assertEquals(termId, terms[i].getTermId());

                }

                // check the reverse mapping (id -> term)
                if (terms[i] instanceof BNode) {

                    // the bnode ID is not preserved.
                    assertTrue(store.getTerm(termId) instanceof BNode);

                } else {

                    assertEquals("reverse mapping", terms[i], store
                            .getTerm(termId));

                }

            }

            dumpTerms(store);
        
            /*
             * verify that we can detect literals by examining the term
             * identifier.
             */

            assertTrue(AbstractTripleStore.isLiteral(store.getTermId(new LiteralImpl("abc"))));
            
            assertTrue(AbstractTripleStore.isLiteral(store.getTermId(new LiteralImpl("abc",XMLSchema.DECIMAL))));
            
            assertTrue(AbstractTripleStore.isLiteral(store.getTermId(new LiteralImpl("abc", "en"))));

            assertFalse(AbstractTripleStore.isLiteral(store.getTermId(new URIImpl("http://www.bigdata.com"))));

            assertFalse(AbstractTripleStore.isLiteral(store.getTermId(RDF.TYPE)));

            assertFalse(AbstractTripleStore.isLiteral(store.getTermId(new BNodeImpl(UUID.randomUUID().toString()))));
            
            assertFalse(AbstractTripleStore.isLiteral(store.getTermId(new BNodeImpl("a12"))));

        } finally {

            store.closeAndDelete();
            
        }

    }

    /**
     * Unit test presents an array of {@link BigdataValue}s that contains
     * duplicates and verifies that the assigned term identifiers are
     * consistent.
     */
    public void test_addTermsFiltersDuplicates() {

        final AbstractTripleStore store = getStore();

        try {
        
            final BigdataValueFactory valueFactory = store.getValueFactory();
            final BigdataURI x = valueFactory.createURI("http://www.foo.org/x");
            final BigdataURI y = valueFactory.createURI("http://www.foo.org/y");
            final BigdataLiteral z = valueFactory.createLiteral("z");
            final BigdataBNode b = valueFactory.createBNode();
            final BigdataBNode c = valueFactory.createBNode("_c");
            
            // add terms - lots of duplicates here.
            store.addTerms(new BigdataValue[] { x, y, b, c, x, b, z, z, c, y, x, });
            
            // none are NULL.
            assertNotSame(x.getTermId(),NULL);
            assertNotSame(y.getTermId(),NULL);
            assertNotSame(z.getTermId(),NULL);
            assertNotSame(b.getTermId(),NULL);
            assertNotSame(c.getTermId(),NULL);

            // all distinct.
            assertNotSame(x.getTermId(), y.getTermId());
            assertNotSame(x.getTermId(), z.getTermId());
            assertNotSame(x.getTermId(), b.getTermId());
            assertNotSame(x.getTermId(), c.getTermId());
            
            assertNotSame(y.getTermId(), z.getTermId());
            assertNotSame(y.getTermId(), b.getTermId());
            assertNotSame(y.getTermId(), c.getTermId());
            
            assertNotSame(z.getTermId(), b.getTermId());
            assertNotSame(z.getTermId(), c.getTermId());
            
            assertNotSame(b.getTermId(), c.getTermId());

            // correct type of term identifier was assigned.
            assertTrue(AbstractTripleStore.isURI(x.getTermId()));
            assertTrue(AbstractTripleStore.isURI(y.getTermId()));
            assertTrue(AbstractTripleStore.isLiteral(z.getTermId()));
            assertTrue(AbstractTripleStore.isBNode(b.getTermId()));
            assertTrue(AbstractTripleStore.isBNode(c.getTermId()));
            
            // reverse lookup is consistent with the assigned term identifiers.
            assertEquals(x,store.getTerm(x.getTermId()));
            assertEquals(y,store.getTerm(y.getTermId()));
            assertEquals(z,store.getTerm(z.getTermId()));
            assertTrue(store.getTerm(b.getTermId()) instanceof BNode);
            assertTrue(store.getTerm(c.getTermId()) instanceof BNode);
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
    /**
     * Unit test verifies that {@link BigdataValue}s whose term identifiers
     * have already been assigned are unchanged by
     * {@link LexiconRelation#addTerms(BigdataValue[], int, boolean)}.
     * <p>
     * Note: {@link BNode}s are not tested for this property since they will be
     * assigned a new term identifier each time. Blank nodes are ONLY made
     * self-consistent within a canonicalizing mapping imposed for some blank
     * node context, e.g., a source document that is being parsed.
     */
    public void test_addTermsFiltersAlreadyAssigned() {

        final AbstractTripleStore store = getStore();

        try {
        
            final BigdataValueFactory valueFactory = store.getValueFactory();
            
            final BigdataURI x = valueFactory.createURI("http://www.foo.org/x");
            final BigdataURI y = valueFactory.createURI("http://www.foo.org/y");
            final BigdataLiteral z = valueFactory.createLiteral("z");
            
            // add terms - lots of duplicates here.
            store.addTerms(new BigdataValue[] { x, y, x, z, y, z});
            
            final long _x = x.getTermId();
            final long _y = y.getTermId();
            final long _z = z.getTermId();
            
            // none are NULL.
            assertNotSame(x.getTermId(),NULL);
            assertNotSame(y.getTermId(),NULL);
            assertNotSame(z.getTermId(),NULL);

            // all distinct.
            assertNotSame(x.getTermId(), y.getTermId());
            assertNotSame(x.getTermId(), z.getTermId());
            
            assertNotSame(y.getTermId(), z.getTermId());
            
            // correct type of term identifier was assigned.
            assertTrue(AbstractTripleStore.isURI(x.getTermId()));
            assertTrue(AbstractTripleStore.isURI(y.getTermId()));
            assertTrue(AbstractTripleStore.isLiteral(z.getTermId()));
            
            // reverse lookup is consistent with the assigned term identifiers.
            assertEquals(x,store.getTerm(x.getTermId()));
            assertEquals(y,store.getTerm(y.getTermId()));
            assertEquals(z,store.getTerm(z.getTermId()));

            /*
             * re-adding the same term instances does not change their term
             * identifer assignments.
             */
            
            // add terms - lots of duplicates here.
            store.addTerms(new BigdataValue[] { x, y, x, z, y, z});

            assertEquals(_x, x.getTermId());
            assertEquals(_y, y.getTermId());
            assertEquals(_z, z.getTermId());
            
            /*
             * verify that re-adding distinct term identifier instances having
             * the same data assigns the same term identifiers.
             */
            {

                final BigdataURI x1 = valueFactory.createURI("http://www.foo.org/x");
                final BigdataURI y1 = valueFactory.createURI("http://www.foo.org/y");
                final BigdataLiteral z1 = valueFactory.createLiteral("z");
                
                // add terms - lots of duplicates here.
                store.addTerms(new BigdataValue[] { x1, y1, x1, z1, y1, z1});

                // same term identifiers were assigned.
                assertEquals(_x, x1.getTermId());
                assertEquals(_y, y1.getTermId());
                assertEquals(_z, z1.getTermId());

            }
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }

    /**
     * Test of {@link ITripleStore#addStatement(Resource, URI, Value)} and
     * {@link ITripleStore#hasStatement(Resource, URI, Value)}.
     */
    public void test_statements() {

        AbstractTripleStore store = getStore();

        try {
        
            final boolean storeBlankNodes = store.getLexiconRelation()
                    .isStoreBlankNodes();
            
            URI x = new URIImpl("http://www.foo.org/x");
            URI y = new URIImpl("http://www.foo.org/y");
            URI z = new URIImpl("http://www.foo.org/z");
    
            URI A = new URIImpl("http://www.foo.org/A");
            URI B = new URIImpl("http://www.foo.org/B");
            URI C = new URIImpl("http://www.foo.org/C");
    
            URI rdfType = RDF.TYPE;
    
            URI rdfsSubClassOf = RDFS.SUBCLASSOF;
    
            Literal lit1 = new LiteralImpl("abc");
            Literal lit2 = new LiteralImpl("abc", A);
            Literal lit3 = new LiteralImpl("abc", "en");
    
            BNode bn1 = new BNodeImpl(UUID.randomUUID().toString());
            BNode bn2 = new BNodeImpl("a12");
    
            store.addStatement(x, rdfType, C);
            
            store.addStatement(y, rdfType, B);
            store.addStatement(z, rdfType, A);
    
            store.addStatement(B, rdfsSubClassOf, A);
            store.addStatement(C, rdfsSubClassOf, B);
    
            final long x_termId;
            {
    
                /*
                 * Make sure that lookup with a different instance succeeds (this
                 * defeats the caching of the termId on the _Value).
                 */
    
                x_termId = store.getTermId(new URIImpl("http://www.foo.org/x"));
    
                assertTrue(x_termId != NULL);
    
            }
    
            final long x_id = store.getTermId(x);
            assertTrue(x_id != NULL);
            assertEquals(x_termId, x_id);
            final long y_id = store.getTermId(y);
            assertTrue(y_id != NULL);
            final long z_id = store.getTermId(z);
            assertTrue(z_id != NULL);
            final long A_id = store.getTermId(A);
            assertTrue(A_id != NULL);
            final long B_id = store.getTermId(B);
            assertTrue(B_id != NULL);
            final long C_id = store.getTermId(C);
            assertTrue(C_id != NULL);
            final long rdfType_id = store.getTermId(rdfType);
            assertTrue(rdfType_id != NULL);
            final long rdfsSubClassOf_id = store.getTermId(rdfsSubClassOf);
            assertTrue(rdfsSubClassOf_id != NULL);
    
            final long lit1_id = store.addTerm(lit1);
            assertTrue(lit1_id != NULL);
            final long lit2_id = store.addTerm(lit2);
            assertTrue(lit2_id != NULL);
            final long lit3_id = store.addTerm(lit3);
            assertTrue(lit3_id != NULL);
    
            final long bn1_id = store.addTerm(bn1);
            assertTrue(bn1_id != NULL);
            final long bn2_id = store.addTerm(bn2);
            assertTrue(bn2_id != NULL);
    
            assertEquals("#terms", 8 + 3 + (store.getLexiconRelation()
                    .isStoreBlankNodes() ? 2 : 0), store.getTermCount());
            assertEquals("#uris", 8, store.getURICount());
            assertEquals("#lits", 3, store.getLiteralCount());
            if (storeBlankNodes) {
                assertEquals("#bnodes", 2, store.getBNodeCount());
            } else {
                // Note: None found since not stored.
                assertEquals("#bnodes", 0, store.getBNodeCount());
            }
    
            assertEquals(x_id, store.getTermId(x));
            assertEquals(y_id, store.getTermId(y));
            assertEquals(z_id, store.getTermId(z));
            assertEquals(A_id, store.getTermId(A));
            assertEquals(B_id, store.getTermId(B));
            assertEquals(C_id, store.getTermId(C));
            assertEquals(rdfType_id, store.getTermId(rdfType));
            assertEquals(rdfsSubClassOf_id, store.getTermId(rdfsSubClassOf));
    
            assertEquals("statementCount", 5, store.getSPOIndex().rangeCount(null,
                    null));
            assertEquals("statementCount", 5, store.getPOSIndex().rangeCount(null,
                    null));
            assertEquals("statementCount", 5, store.getOSPIndex().rangeCount(null,
                    null));
            assertTrue(store.hasStatement(x, rdfType, C));
            assertTrue(store.hasStatement(y, rdfType, B));
            assertTrue(store.hasStatement(z, rdfType, A));
            assertTrue(store.hasStatement(B, rdfsSubClassOf, A));
            assertTrue(store.hasStatement(C, rdfsSubClassOf, B));
    
            store.commit();// Note: Should not make any difference.
    
            assertEquals("statementCount", 5, store.getSPOIndex().rangeCount(null,
                    null));
            assertEquals("statementCount", 5, store.getPOSIndex().rangeCount(null,
                    null));
            assertEquals("statementCount", 5, store.getOSPIndex().rangeCount(null,
                    null));
            assertTrue(store.hasStatement(x, rdfType, C));
            assertTrue(store.hasStatement(y, rdfType, B));
            assertTrue(store.hasStatement(z, rdfType, A));
            assertTrue(store.hasStatement(B, rdfsSubClassOf, A));
            assertTrue(store.hasStatement(C, rdfsSubClassOf, B));
    
            assertEquals("#terms", 8 + 3 + (storeBlankNodes ? 2 : 0), store.getTermCount());
            assertEquals("#uris", 8, store.getURICount());
            assertEquals("#lits", 3, store.getLiteralCount());
            if (storeBlankNodes) {
                assertEquals("#bnodes", 2, store.getBNodeCount());
            } else {
                // Note: None found since not stored.
                assertEquals("#bnodes", 0, store.getBNodeCount());
            }
    
        } finally {

            store.closeAndDelete();
            
        }

    }
    
    /**
     * Verify that we can locate a statement that we add to the database using
     * each statement index.
     */
    public void test_addLookup_nativeAPI() {
            
        AbstractTripleStore store = getStore();
        
        try {

            // verify nothing in the store.
            assertSameIterator(new SPO[]{},
                    store.getAccessPath(NULL,NULL,NULL).iterator());
            
            final BigdataValueFactoryImpl valueFactory = store.getValueFactory();
            final BigdataURI A = valueFactory.createURI("http://www.bigdata.com/A");
            final BigdataURI B = valueFactory.createURI("http://www.bigdata.com/B");
            final BigdataURI C = valueFactory.createURI("http://www.bigdata.com/C");
            final BigdataURI rdfType = valueFactory.createURI(RDF.TYPE.stringValue());

            store.addTerms(new BigdataValue[] { A, B, C, rdfType });
            
            store.addTerm(A);
            store.addTerm(B);
            store.addTerm(C);
            store.addTerm(rdfType);
            
            /*
             * Insert two statements into the store.
             */
            
            final SPO spo1 = new SPO(A, rdfType, B, StatementEnum.Explicit);            

            final SPO spo2 = new SPO(A, rdfType, C, StatementEnum.Explicit);            

            if(log.isInfoEnabled()) {

                log.info("adding statements");
            
                log.info("spo1: "+spo1);

                log.info("spo2: "+spo2);
                
            }
            
            assertEquals("mutationCount", 2L,//
                    store.addStatements(new SPO[] { spo1, spo2 }, 2)//
                    );

            // verify that a re-insert reports a zero mutation count.
            assertEquals("mutationCount", 0L,//
                    store.addStatements(new SPO[] { spo1, spo2 }, 2)//
                    );

            if (log.isInfoEnabled()) {

                log.info("\n" + store.dumpStore());
            
                log.info("\nSPO\n"
                        + store.getSPORelation().dump(SPOKeyOrder.SPO));
                
                log.info("\nPOS\n"
                        + store.getSPORelation().dump(SPOKeyOrder.POS));

                log.info("\nOSP\n"
                        + store.getSPORelation().dump(SPOKeyOrder.OSP));
                
            }

            // verify range count on each of the statement indices.
            assertEquals(2,store.getStatementIndex(SPOKeyOrder.SPO).rangeCount());
            assertEquals(2,store.getStatementIndex(SPOKeyOrder.POS).rangeCount());
            assertEquals(2,store.getStatementIndex(SPOKeyOrder.OSP).rangeCount());

            /*
             * check indices for spo1.
             */
            
            // check the SPO index.
            assertSameSPOs(new SPO[] { spo1, spo2 }, store.getAccessPath(spo1.s, NULL,
                    NULL).iterator());

            // check the POS index.
            assertSameSPOs(new SPO[] { spo1, spo2 }, store.getAccessPath(NULL, spo1.p,
                    NULL).iterator());

            // check the OSP index.
            assertSameSPOs(new SPO[] { spo1 }, store.getAccessPath(NULL, NULL,
                    spo1.o).iterator());
            
            /*
             * check indices for spo2 (only differs in the object position).
             */
            
            // check the OSP index.
            assertSameSPOs(new SPO[] { spo2 }, store.getAccessPath(NULL, NULL,
                    spo2.o).iterator());
            
            /*
             * full range scan (uses the SPO index).
             */

            assertSameSPOs(new SPO[] { spo1, spo2 }, store.getAccessPath(NULL, NULL,
                    NULL).iterator());
            
            /*
             * Remove spo1 from the statement from the store.
             * 
             * Note: indices that support isolation will still report the SAME
             * range count after the statement has been removed.
             */
            
            log.info("Removing 1st statement.");
            
            assertEquals(1,store.getAccessPath(spo1.s, spo1.p, spo1.o).removeAll());

            if(log.isInfoEnabled()) {

                log.info("\n"+store.dumpStore());

                log.info("\nSPO\n"+store.getSPORelation().dump(SPOKeyOrder.SPO));
                log.info("\nPOS\n"+store.getSPORelation().dump(SPOKeyOrder.POS));
                log.info("\nOSP\n"+store.getSPORelation().dump(SPOKeyOrder.OSP));
                
            }

            /*
             * verify that the statement is gone from each of the statement
             * indices.
             */
            
            // check the SPO index.
            assertSameSPOs(new SPO[] {spo2}, store.getAccessPath(spo1.s, NULL,
                    NULL).iterator());

            // check the POS index.
            assertSameSPOs(new SPO[] {spo2}, store.getAccessPath(NULL, spo1.p,
                    NULL).iterator());

            // check the OSP index.
            assertSameSPOs(new SPO[] {}, store.getAccessPath(NULL, NULL,
                    spo1.o).iterator());

            /*
             * full range scan (uses the SPO index).
             */

            assertSameSPOs(new SPO[] { spo2 }, store.getAccessPath(NULL, NULL,
                    NULL).iterator());

            /*
             * remove all statements (only spo2 remains).
             */
            
            log.info("Removing all statements.");

            assertEquals(1,store.getAccessPath(NULL,NULL,NULL).removeAll());

            if(log.isInfoEnabled()) {

                log.info("\n"+store.dumpStore());

                log.info("\nSPO\n"+store.getSPORelation().dump(SPOKeyOrder.SPO));
                log.info("\nPOS\n"+store.getSPORelation().dump(SPOKeyOrder.POS));
                log.info("\nOSP\n"+store.getSPORelation().dump(SPOKeyOrder.OSP));
                
            }

            /*
             * verify that the statement is gone from each of the statement
             * indices.
             */
            
            // check the SPO index.
            assertSameSPOs(new SPO[] {}, store.getAccessPath(spo2.s, NULL,
                    NULL).iterator());

            // check the POS index.
            assertSameSPOs(new SPO[] {}, store.getAccessPath(NULL, spo2.p,
                    NULL).iterator());

            // check the OSP index.
            assertSameSPOs(new SPO[] {}, store.getAccessPath(NULL, NULL,
                    spo2.o).iterator());

            /*
             * full range scan (uses the SPO index).
             */

            assertSameSPOs(new SPO[] {}, store.getAccessPath(NULL, NULL,
                    NULL).iterator());
            
        } finally {
            
            store.closeAndDelete();
            
        }

    }
    
    /**
     * Test the ability to add and remove statements using both fully
     * bound and partly bound triple patterns using the native API.
     */
    public void test_addRemove_nativeAPI() {
        
        AbstractTripleStore store = getStore();
        
        try {

            // verify nothing in the store.
            assertSameIterator(new SPO[]{},
                    store.getAccessPath(NULL,NULL,NULL).iterator());

            final BigdataValueFactory f = store.getValueFactory();
            
            final BigdataURI A = f.createURI("http://www.bigdata.com/A");
            final BigdataURI B = f.createURI("http://www.bigdata.com/B");
            final BigdataURI C = f.createURI("http://www.bigdata.com/C");
            final BigdataURI rdfType = f.asValue(RDF.TYPE);
            
            // assign term identifiers for reuse below.
            store.addTerms(new BigdataValue[] { A, B, C, rdfType });
            
            {
                // load statements into db.
                IStatementBuffer buffer = new StatementBuffer(store, 2);

                buffer.add(A, rdfType, B);
                buffer.add(A, rdfType, C);

                buffer.flush();
            }

            // store.commit();
            
            if (log.isInfoEnabled())
                log.info("\n" + store.dumpStore());
            
            // make sure that term identifiers were assigned as a side-effect.
            assertTrue(new SPO(A, rdfType, B, StatementEnum.Explicit).isFullyBound());
            
            assertSameSPOs(new SPO[] {
                    new SPO(A, rdfType, B, StatementEnum.Explicit),
                    new SPO(A, rdfType, C, StatementEnum.Explicit),//
                    },//
                    store.getAccessPath(NULL, NULL, NULL).iterator());

            assertEquals(1, store.getAccessPath(NULL, NULL, store.getTermId(B))
                    .removeAll());

            if (log.isInfoEnabled()) {
                log.info("\n" + store.dumpStore());
            }
            
//            store.commit();
            
            assertSameSPOs(new SPO[] {//
                    new SPO(A, rdfType, C, StatementEnum.Explicit), //
                    }, //
                    store.getAccessPath(NULL,NULL,NULL).iterator()
                    );

        } finally {
            
            store.closeAndDelete();
            
        }
        
    }

    /**
     * Test using the native API of adding explicit, inferred, and axiom
     * {@link SPO}s.
     */
    public void test_addInferredExplicitAxiom() {

        final AbstractTripleStore store = getStore();
        
        try {
            
            final BigdataValueFactory f = store.getValueFactory();

            final BigdataURI v1 = f.createURI("http://www.bigdata.com/1"); 
            final BigdataURI v2 = f.createURI("http://www.bigdata.com/2"); 
            final BigdataURI v3 = f.createURI("http://www.bigdata.com/3"); 
            
            // assign term identifiers.
            store.addTerms(new BigdataValue[]{v1,v2,v3});
            
            final SPO[] a = new SPO[] {
            
            new SPO(v1, v2, v3, StatementEnum.Explicit),
            new SPO(v2, v2, v3, StatementEnum.Inferred),
            new SPO(v3, v2, v3, StatementEnum.Axiom)
            };

            store.addStatements(a,a.length);
            
            assertSameSPOs(new SPO[] {//
                    new SPO(v1, v2, v3, StatementEnum.Explicit),//
                    new SPO(v2, v2, v3, StatementEnum.Inferred),//
                    new SPO(v3, v2, v3, StatementEnum.Axiom),//
                    },//
                    store.getAccessPath(NULL,NULL,NULL).iterator()
                    );

            if (log.isInfoEnabled())
                log.info("\n" + store.dumpStore());

        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
    /**
     * Test the ability to add and remove statements using both fully bound and
     * partly bound triple patterns using the Sesame compatible API.
     */
    public void test_addRemove_sesameAPI() throws SailException {
        
        AbstractTripleStore store = getStore();
        
        try {

            // verify nothing in the store.
            assertSameIterator(new Statement[]{},
                    store.getAccessPath(null,null,null).iterator());
            
            final BigdataValueFactory f = store.getValueFactory();
            
            final BigdataURI A = f.createURI("http://www.bigdata.com/A");
            final BigdataURI B = f.createURI("http://www.bigdata.com/B");
            final BigdataURI C = f.createURI("http://www.bigdata.com/C");
            final BigdataURI rdfType = f.asValue(RDF.TYPE);
            
            {
                IStatementBuffer buffer = new StatementBuffer(store, 100);

                buffer.add(A, rdfType, B);
                buffer.add(A, rdfType, C);

                buffer.flush();
            }

            assertSameStatements(new Statement[]{
                    new StatementImpl(A,RDF.TYPE,B),
                    new StatementImpl(A,RDF.TYPE,C),
                    },
                    store.getStatements(null,null,null)
                    );

            if (log.isInfoEnabled())
                log.info("\n" + store.dumpStore());
            
            assertEquals(1L, store.removeStatements(null, null, B));

            if (log.isInfoEnabled())
                log.info("\n" + store.dumpStore());
            
            assertSameStatements(new Statement[]{
                    new StatementImpl(A,rdfType,C),
                    },
                    store.getStatements(null,null,null)
                    );

        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
    /**
     * Test of
     * {@link IRawTripleStore#removeStatements(com.bigdata.relation.accesspath.IChunkedOrderedIterator)}
     * 
     * FIXME verify that there is a variant of this test with statement
     * identifiers enabled in order to make sure that we are testing the closure
     * computation over the statement identifiers.
     */
    public void test_removeStatements() {
        
        AbstractTripleStore store = getStore();
        
        try {

            /*
             * Setup some terms.
             * 
             * Note: we need the store to assign the term identifiers since they
             * are bit marked as to their type (uri, literal, bnode, or
             * statement identifier).
             */

            final BigdataValueFactory f = store.getValueFactory();

            final BigdataURI x = f.createURI("http://www.foo.org/x1");
            final BigdataURI y = f.createURI("http://www.foo.org/y2");
            final BigdataURI z = f.createURI("http://www.foo.org/z3");
    
            store.addTerms(new BigdataValue[] { x, y, z });
            
            final long x1 = x.getTermId();
            final long y2 = y.getTermId();
            final long z3 = z.getTermId();
            
            // verify nothing in the store.
            assertSameIterator(new Statement[]{},
                    store.getAccessPath(null,null,null).iterator());
            
//            SPOAssertionBuffer buffer = new SPOAssertionBuffer(store, store,
//                    null/* filter */, 100/* capacity */, false/*justify*/);
            
            SPO[] a = new SPO[] {

                    new SPO(x1, y2, z3,StatementEnum.Explicit),
                    
                    new SPO(y2, y2, z3,StatementEnum.Explicit)
            
            };
            
            store.addStatements(a,a.length);
            
//            buffer.flush();

            assertTrue(store.hasStatement(x1,y2,z3));
            assertTrue(store.hasStatement(y2,y2,z3));
            assertEquals(2,store.getStatementCount());
            
            assertEquals(1, store.removeStatements(new ChunkedArrayIterator<ISPO>(
                    1,
                    new SPO[] {
                            new SPO(x1, y2, z3, StatementEnum.Explicit)
                            },
                    null/*keyOrder*/
                    ), false/* computeClosureForStatementIdentifiers */));

            assertFalse(store.hasStatement(x1,y2,z3));
            assertTrue(store.hasStatement(y2,y2,z3));

        } finally {
            
            store.closeAndDelete();
            
        }

    }
    
}
