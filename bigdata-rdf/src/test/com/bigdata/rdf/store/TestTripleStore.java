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

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;
import org.openrdf.vocabulary.XmlSchema;

import com.bigdata.rdf.inf.SPOAssertionBuffer;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOArrayIterator;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Test basic features of the {@link ITripleStore} API.
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
    protected void doAddTermTest(AbstractTripleStore store, _Value term) {

        assertEquals(NULL, store.getTermId(term));

        final long id = store.addTerm(term);

        assertNotSame(NULL, id);

        assertEquals(id, store.getTermId(term));

        assertEquals(term, store.getTerm(id));

        assertEquals(id, store.addTerm(term));

    }

    /**
     * Simple test of inserting one term at a time into the lexicon.
     */
    public void test_addTerm() {

        AbstractTripleStore store = getStore();
        
        try {

            doAddTermTest(store, new _Literal("abc"));
            doAddTermTest(store, new _Literal("abc", new _URI(XmlSchema.DECIMAL)));
            doAddTermTest(store, new _Literal("abc", "en"));
    
            doAddTermTest(store, new _URI("http://www.bigdata.com"));
            doAddTermTest(store, new _URI(RDF.TYPE));
            doAddTermTest(store, new _URI(RDFS.SUBCLASSOF));
            doAddTermTest(store, new _URI(XmlSchema.DECIMAL));
    
            doAddTermTest(store, new _BNode(UUID.randomUUID().toString()));
            doAddTermTest(store, new _BNode("a12"));
    
            store.commit();
    
            dumpTerms(store);

            /*
             * verify that we can detect literals by examining the term identifier.
             */

            assertTrue(store.isLiteral(store.getTermId(new _Literal("abc"))));
            
            assertTrue(store.isLiteral(store.getTermId(new _Literal("abc",new _URI(XmlSchema.DECIMAL)))));
            
            assertTrue(store.isLiteral(store.getTermId(new _Literal("abc", "en"))));

            assertFalse(store.isLiteral(store.getTermId(new _URI("http://www.bigdata.com"))));

            assertFalse(store.isLiteral(store.getTermId(new _URI(RDF.TYPE))));

            assertFalse(store.isLiteral(store.getTermId(new _BNode(UUID.randomUUID().toString()))));
            
            assertFalse(store.isLiteral(store.getTermId(new _BNode("a12"))));
            
        } finally {
            
            store.closeAndDelete();

        }

    }

    /**
     * Simple test of batch insertion of terms into the lexicon.
     */
    public void test_insertTerms() {

        AbstractTripleStore store = getStore();

        try {
        
            _Value[] terms = new _Value[] {//
    
            new _URI("http://www.bigdata.com"),//
    
                    new _URI(RDF.TYPE),//
                    new _URI(RDFS.SUBCLASSOF),//
                    new _URI(XmlSchema.DECIMAL),//
    
                    new _Literal("abc"),//
                    new _Literal("abc", new _URI(XmlSchema.DECIMAL)),//
                    new _Literal("abc", "en"),//
    
                    new _BNode(UUID.randomUUID().toString()),//
                    new _BNode("a12") //
            };
    
            store.insertTerms(terms, terms.length, false/* haveKeys */, false/* sorted */);
    
            store.commit();
            
            for (int i = 0; i < terms.length; i++) {
    
                // verify set by side-effect on batch insert.
                assertNotSame(NULL,terms[i].termId);
                assertTrue(terms[i].known);
                
                // save & clear
                final long termId = terms[i].termId;
                terms[i].termId = NULL;
                terms[i].known = false;
                if(i%2==0) terms[i].key = null;
                
                // check the forward mapping (term -> id)
                assertEquals("forward mapping", termId, store.getTermId(terms[i]));
    
                // verify set by side effect.
                assertEquals(termId,terms[i].termId);
                assertTrue(terms[i].known);
                assertNotNull(terms[i].key);
    
                // check the reverse mapping (id -> term)
                assertEquals("reverse mapping", terms[i], store.getTerm(termId));
    
            }
    
            dumpTerms(store);
        
            /*
             * verify that we can detect literals by examining the term identifier.
             */

            assertTrue(store.isLiteral(store.getTermId(new _Literal("abc"))));
            
            assertTrue(store.isLiteral(store.getTermId(new _Literal("abc",new _URI(XmlSchema.DECIMAL)))));
            
            assertTrue(store.isLiteral(store.getTermId(new _Literal("abc", "en"))));

            assertFalse(store.isLiteral(store.getTermId(new _URI("http://www.bigdata.com"))));

            assertFalse(store.isLiteral(store.getTermId(new _URI(RDF.TYPE))));

            assertFalse(store.isLiteral(store.getTermId(new _BNode(UUID.randomUUID().toString()))));
            
            assertFalse(store.isLiteral(store.getTermId(new _BNode("a12"))));

        } finally {

            store.closeAndDelete();
            
        }

    }

//    /**
//     * @todo this does not actually test anything.
//     *
//     */
//    public void test_insertTermsWithDuplicates() {
//        
//        ITripleStore store = getStore();
//
//        _Value[] terms = new _Value[] {//
//
//                new _URI("http://www.bigdata.com"),//
//                new _URI("http://www.bigdata.com"),//
//
////                new _URI(RDF.TYPE),//
////                new _URI(RDFS.SUBCLASSOF),//
////                new _URI(XmlSchema.DECIMAL),//
////
////                new _Literal("abc"),//
////                new _Literal("abc", new _URI(XmlSchema.DECIMAL)),//
////                new _Literal("abc", "en"),//
////
////                new _BNode(UUID.randomUUID().toString()),//
////                new _BNode("a12") //
//        };
//
//        store.insertTerms(terms, terms.length, false/* haveKeys */, false/* sorted */);
//
//        store.commit();
//        
//        dumpTerms(store);
//
//        store.closeAndDelete();
//
//    }
    
    /**
     * Test of {@link ITripleStore#addStatement(Resource, URI, Value)} and
     * {@link ITripleStore#hasStatement(Resource, URI, Value)}.
     */
    public void test_statements() {

        AbstractTripleStore store = getStore();

        try {
        
            _URI x = new _URI("http://www.foo.org/x");
            _URI y = new _URI("http://www.foo.org/y");
            _URI z = new _URI("http://www.foo.org/z");
    
            _URI A = new _URI("http://www.foo.org/A");
            _URI B = new _URI("http://www.foo.org/B");
            _URI C = new _URI("http://www.foo.org/C");
    
            _URI rdfType = new _URI(RDF.TYPE);
    
            _URI rdfsSubClassOf = new _URI(RDFS.SUBCLASSOF);
    
            _Literal lit1 = new _Literal("abc");
            _Literal lit2 = new _Literal("abc", A);
            _Literal lit3 = new _Literal("abc", "en");
    
            _BNode bn1 = new _BNode(UUID.randomUUID().toString());
            _BNode bn2 = new _BNode("a12");
    
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
    
                x_termId = store.getTermId(new _URI("http://www.foo.org/x"));
    
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
    
            assertEquals("#terms", 8 + 3 + 2, store.getTermCount());
            assertEquals("#uris", 8, store.getURICount());
            assertEquals("#lits", 3, store.getLiteralCount());
            assertEquals("#bnodes", 2, store.getBNodeCount());
    
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
    
            store.commit();
    
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
    
            assertEquals("#terms", 8 + 3 + 2, store.getTermCount());
            assertEquals("#uris", 8, store.getURICount());
            assertEquals("#lits", 3, store.getLiteralCount());
            assertEquals("#bnodes", 2, store.getBNodeCount());

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
            
            _URI A = new _URI("http://www.bigdata.com/A");
            _URI B = new _URI("http://www.bigdata.com/B");
            _URI C = new _URI("http://www.bigdata.com/C");
            _URI rdfType = new _URI(RDF.TYPE);

            store.addTerm(A);
            store.addTerm(B);
            store.addTerm(C);
            store.addTerm(rdfType);

            /*
             * Insert two statements into the store.
             */
            
            SPO spo1 = new SPO(A.termId, rdfType.termId, B.termId,
                    StatementEnum.Explicit);            

            SPO spo2 = new SPO(A.termId, rdfType.termId, C.termId,
                    StatementEnum.Explicit);            

            System.err.println("adding statements");
            
            store.addStatements(new SPO[]{spo1,spo2}, 2);

            store.usage();
            
            store.dumpStore();

            // verify range count on each of the statement indices.
            assertEquals(2,store.getStatementIndex(KeyOrder.SPO).rangeCount(null, null));
            assertEquals(2,store.getStatementIndex(KeyOrder.POS).rangeCount(null, null));
            assertEquals(2,store.getStatementIndex(KeyOrder.OSP).rangeCount(null, null));

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
            
            System.err.println("Removing 1st statement.");
            
            assertEquals(1,store.getAccessPath(spo1.s, spo1.p, spo1.o).removeAll());

            store.usage();
            
            store.dumpStore();

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
            
            System.err.println("Removing all statements.");

            assertEquals(1,store.getAccessPath(NULL,NULL,NULL).removeAll());

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

            store.usage();
            
            store.dumpStore();
            
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
            
            URIImpl A = new URIImpl("http://www.bigdata.com/A");
            URIImpl B = new URIImpl("http://www.bigdata.com/B");
            URIImpl C = new URIImpl("http://www.bigdata.com/C");
            
            IStatementBuffer buffer = new StatementBuffer(store,100);
            
            buffer.add(A, URIImpl.RDF_TYPE, B);
            buffer.add(A, URIImpl.RDF_TYPE, C);
            
            buffer.flush();

            store.commit();
            
            assertSameSPOs(new SPO[] {
                    new SPO(store.getTermId(A), store
                            .getTermId(URIImpl.RDF_TYPE), store.getTermId(B),
                            StatementEnum.Explicit),
                    new SPO(store.getTermId(A), store
                            .getTermId(URIImpl.RDF_TYPE), store.getTermId(C),
                            StatementEnum.Explicit), },
                    store.getAccessPath(NULL,NULL,NULL).iterator()
                    );

            store.dumpStore();
            
            assertEquals(1, store.getAccessPath(NULL, NULL, store.getTermId(B))
                    .removeAll());

//            store.dumpStore();
            
            store.commit();
            
            store.dumpStore();
            
            assertSameSPOs(new SPO[] {
                    new SPO(store.getTermId(A), store
                            .getTermId(URIImpl.RDF_TYPE), store.getTermId(C),
                            StatementEnum.Explicit), },
                    store.getAccessPath(NULL,NULL,NULL).iterator()
                    );

        } finally {
            
            store.closeAndDelete();
            
        }
        
    }

    /**
     * Test using the nation API of adding explicit, inferred, and axiom
     * {@link SPO}s.
     */
    public void test_addInferredExplicitAxiom() {

        AbstractTripleStore store = getStore();
        
        try {
            
            SPOAssertionBuffer buffer = new SPOAssertionBuffer(store,
                    null/* filter */, 100/* capacity */, false/*justified*/);
            
            buffer.add(new SPO(1,2,3,StatementEnum.Explicit));
            buffer.add(new SPO(2,2,3,StatementEnum.Inferred));
            buffer.add(new SPO(3,2,3,StatementEnum.Axiom));
            
            buffer.flush();

            store.commit();
            
            assertSameSPOs(new SPO[] {
                    new SPO(1,2,3,StatementEnum.Explicit),
                    new SPO(2, 2, 3, StatementEnum.Inferred),
                    new SPO(3, 2, 3, StatementEnum.Axiom),
                    },
                    store.getAccessPath(NULL,NULL,NULL).iterator()
                    );

            store.dumpStore();
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
    /**
     * Test the ability to add and remove statements using both fully bound and
     * partly bound triple patterns using the Sesame compatible API.
     */
    public void test_addRemove_sesameAPI() {
        
        AbstractTripleStore store = getStore();
        
        try {

            // verify nothing in the store.
            assertSameIterator(new Statement[]{},
                    store.getAccessPath(null,null,null).iterator());
            
            URIImpl A = new URIImpl("http://www.bigdata.com/A");
            URIImpl B = new URIImpl("http://www.bigdata.com/B");
            URIImpl C = new URIImpl("http://www.bigdata.com/C");
            
            IStatementBuffer buffer = new StatementBuffer(store,100);
            
            buffer.add(A, URIImpl.RDF_TYPE, B);
            buffer.add(A, URIImpl.RDF_TYPE, C);
            
            buffer.flush();

            assertSameStatements(new Statement[]{
                    new StatementImpl(A,URIImpl.RDF_TYPE,B),
                    new StatementImpl(A,URIImpl.RDF_TYPE,C),
                    },
                    store.getStatements(null,null,null)
                    );

            store.dumpStore();
            
            assertEquals(1,store.removeStatements(null, null, B));

            store.dumpStore();
            
            assertSameStatements(new Statement[]{
                    new StatementImpl(A,URIImpl.RDF_TYPE,C),
                    },
                    store.getStatements(null,null,null)
                    );

        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
    /**
     * Test of {@link IRawTripleStore#removeStatements(com.bigdata.rdf.spo.ISPOIterator)}
     */
    public void test_removeStatements() {
        
        AbstractTripleStore store = getStore();
        
        try {

            // verify nothing in the store.
            assertSameIterator(new Statement[]{},
                    store.getAccessPath(null,null,null).iterator());
            
            SPOAssertionBuffer buffer = new SPOAssertionBuffer(store,
                    null/* filter */, 100/* capacity */, false/*justify*/);
            
            buffer.add(new SPO(1, 2, 3,StatementEnum.Explicit));
            buffer.add(new SPO(2, 2, 3,StatementEnum.Explicit));
            
            buffer.flush();

            assertTrue(store.hasStatement(1,2,3));
            assertTrue(store.hasStatement(2,2,3));
            assertEquals(2,store.getStatementCount());
            
            assertEquals(1, store.removeStatements(new SPOArrayIterator(
                    new SPO[] { new SPO(1, 2, 3, StatementEnum.Explicit) }, 1)));

            assertFalse(store.hasStatement(1,2,3));
            assertTrue(store.hasStatement(2,2,3));

        } finally {
            
            store.closeAndDelete();
            
        }

    }
    
}
