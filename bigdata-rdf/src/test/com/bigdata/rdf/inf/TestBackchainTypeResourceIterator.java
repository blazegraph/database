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
 * Created on Oct 30, 2007
 */

package com.bigdata.rdf.inf;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for {@link BackchainTypeResourceIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBackchainTypeResourceIterator extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestBackchainTypeResourceIterator() {
        super();
    }

    /**
     * @param name
     */
    public TestBackchainTypeResourceIterator(String name) {
        super(name);
    }

    /**
     * Test when only the subject of the triple pattern is bound. In this case
     * the iterator MUST add a single entailment (s rdf:Type rdfs:Resource)
     * unless it is explicitly present in the database.
     */
    public void test_subjectBound() {
     
        AbstractTripleStore store = getStore();
        
        try {

            // adds rdf:Type and rdfs:Resource to the store.
            RDFSHelper vocab = new RDFSHelper(store);

            URI A = new URIImpl("http://www.foo.org/A");
            URI B = new URIImpl("http://www.foo.org/B");
            URI C = new URIImpl("http://www.foo.org/C");

            /*
             * add statements to the store.
             * 
             * Note: this gives us TWO (2) distinct subjects (A and B), but we 
             * will only visit statements for (A).
             */
            
            IStatementBuffer buffer = new StatementBuffer(store, 100/*capacity*/);
            
            buffer.add(A, URIImpl.RDF_TYPE, B);
            buffer.add(B, URIImpl.RDF_TYPE, C);
            
            buffer.flush();

            ISPOIterator itr = new BackchainTypeResourceIterator(//
                    store.getAccessPath(store.getTermId(A), NULL, NULL).iterator(),//
                    store.getTermId(A), NULL, NULL,//
                    store, //
                    vocab.rdfType.id, //
                    vocab.rdfsResource.id //
                    );

            assertSameSPOsAnyOrder(store,
                    
                    new SPO[]{
                    
                    new SPO(//
                            store.getTermId(A),//
                            store.getTermId(URIImpl.RDF_TYPE),//
                            store.getTermId(B),//
                            StatementEnum.Explicit),
                            
                    new SPO(//
                            store.getTermId(A), //
                            store.getTermId(URIImpl.RDF_TYPE), //
                            store.getTermId(URIImpl.RDFS_RESOURCE), //
                            StatementEnum.Inferred)
                    },
                    
                    itr
                    
            );            
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }

    /**
     * Variant test where there is an explicit ( s rdf:type rdfs:Resource ) in
     * the database for the given subject. For this test we verify that the
     * iterator visits an "explicit" statement rather than adding its own
     * inference.
     */
    public void test_subjectBound2() {
     
        AbstractTripleStore store = getStore();
        
        try {

            // adds rdf:Type and rdfs:Resource to the store.
            RDFSHelper vocab = new RDFSHelper(store);

            URI A = new URIImpl("http://www.foo.org/A");
            URI B = new URIImpl("http://www.foo.org/B");
            URI C = new URIImpl("http://www.foo.org/C");

            /*
             * add statements to the store.
             * 
             * Note: this gives us TWO (2) distinct subjects (A and B), but we 
             * will only visit statements for (A).
             */
            
            IStatementBuffer buffer = new StatementBuffer(store, 100/*capacity*/);
            
            buffer.add(A, URIImpl.RDF_TYPE, B);

            buffer.add(A, URIImpl.RDF_TYPE, URIImpl.RDFS_RESOURCE);

            buffer.add(B, URIImpl.RDF_TYPE, C);
            
            buffer.flush();

            store.dumpStore();

            ISPOIterator itr = new BackchainTypeResourceIterator(//
                    store.getAccessPath(store.getTermId(A), NULL, NULL).iterator(),//
                    store.getTermId(A), NULL, NULL,//
                    store, //
                    vocab.rdfType.id, //
                    vocab.rdfsResource.id //
                    );

            assertSameSPOsAnyOrder(store, new SPO[]{
                    
                    new SPO(//
                            store.getTermId(A),//
                            store.getTermId(URIImpl.RDF_TYPE),//
                            store.getTermId(B),//
                            StatementEnum.Explicit),
                            
                    new SPO(//
                            store.getTermId(A), //
                            store.getTermId(URIImpl.RDF_TYPE), //
                            store.getTermId(URIImpl.RDFS_RESOURCE), //
                            StatementEnum.Explicit)
                    },
                
                itr
                );
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }

    /**
     * Test when the triple pattern has no bound variables. In this case the
     * iterator MUST add an entailment for each distinct resource in the store
     * unless there is also an explicit (s rdf:type rdfs:Resource) assertion in
     * the database.
     */
    public void test_noneBound() {
        
        AbstractTripleStore store = getStore();
        
        try {

            // adds rdf:Type and rdfs:Resource to the store.
            RDFSHelper vocab = new RDFSHelper(store);

            URI A = new URIImpl("http://www.foo.org/A");
            URI B = new URIImpl("http://www.foo.org/B");
            URI C = new URIImpl("http://www.foo.org/C");

            /*
             * add statements to the store.
             * 
             * Note: this gives us TWO (2) distinct subjects (A and B). Since
             * nothing is bound in the query we will visit both explicit
             * statements and also the (s type resource) entailments for both
             * distinct subjects.
             */
            
            IStatementBuffer buffer = new StatementBuffer(store, 100/*capacity*/);
            
            buffer.add(A, URIImpl.RDF_TYPE, B);
            
            buffer.add(A, URIImpl.RDF_TYPE, URIImpl.RDFS_RESOURCE);
            
            buffer.add(B, URIImpl.RDF_TYPE, C);
            
            buffer.flush();

            ISPOIterator itr = new BackchainTypeResourceIterator(//
                    store.getAccessPath(NULL, NULL, NULL).iterator(),//
                    NULL, NULL, NULL,//
                    store, //
                    vocab.rdfType.id, //
                    vocab.rdfsResource.id //
                    );

            assertSameSPOsAnyOrder(store, new SPO[]{

                    new SPO(//
                            store.getTermId(A),//
                            store.getTermId(URIImpl.RDF_TYPE),//
                            store.getTermId(B),//
                            StatementEnum.Explicit),

                    new SPO(//
                            store.getTermId(B),//
                            store.getTermId(URIImpl.RDF_TYPE),//
                            store.getTermId(C),//
                            StatementEnum.Explicit),
                    
                    new SPO(//
                            store.getTermId(A), //
                            store.getTermId(URIImpl.RDF_TYPE), //
                            store.getTermId(URIImpl.RDFS_RESOURCE), //
                            StatementEnum.Explicit),
                    
                    new SPO(//
                            store.getTermId(B), //
                            store.getTermId(URIImpl.RDF_TYPE), //
                            store.getTermId(URIImpl.RDFS_RESOURCE), //
                            StatementEnum.Inferred)
                    
            },
            
            itr);
            
        } finally {
            
            store.closeAndDelete();
            
        }

    }
    
    /**
     * Test for other triple patterns (all bound, predicate bound, object bound,
     * etc). In all cases the iterator MUST NOT add any entailments.
     * 
     * @todo this is only testing a single access path.
     */
    public void test_otherBound_01() {
        
        AbstractTripleStore store = getStore();
        
        try {

            // adds rdf:Type and rdfs:Resource to the store.
            RDFSHelper vocab = new RDFSHelper(store);

            URI A = new URIImpl("http://www.foo.org/A");
            URI B = new URIImpl("http://www.foo.org/B");
            URI C = new URIImpl("http://www.foo.org/C");

            /*
             * add statements to the store.
             * 
             * Note: this gives us TWO (2) distinct subjects (A and B).
             */
            
            IStatementBuffer buffer = new StatementBuffer(store, 100/*capacity*/);
            
            buffer.add(A, URIImpl.RDF_TYPE, B);
            buffer.add(B, URIImpl.RDF_TYPE, C);
            
            buffer.flush();

            ISPOIterator itr = new BackchainTypeResourceIterator(//
                    store.getAccessPath(NULL, NULL, store.getTermId(B)).iterator(),//
                    NULL, NULL, store.getTermId(B),//
                    store, //
                    vocab.rdfType.id, //
                    vocab.rdfsResource.id //
                    );

            /*
             * Note: Since we are reading with the object bound, only the
             * explicit statements for that object should make it into the
             * iterator.
             */

            assertSameSPOsAnyOrder(store, new SPO[]{
                    
                    new SPO(//
                            store.getTermId(A),//
                            store.getTermId(URIImpl.RDF_TYPE),//
                            store.getTermId(B),//
                            StatementEnum.Explicit)
                    
                },
                    itr);
            
        } finally {
            
            store.closeAndDelete();
            
        }

    }
    
    /**
     * Backchain test when the subject is both bound and unbound and where the
     * predicate is bound to <code>rdf:type</code> and the object is bound to
     * <code>rdfs:Resource</code>.
     * 
     * FIXME test all access paths, including where the predicate is NULL and
     * where it is rdf:type and where the object is NULL and where it is
     * rdfs:Resource.
     */
    public void test_backchain_foo_type_resource() {

        AbstractTripleStore store = getStore();

        try {

            RDFSHelper vocab = new RDFSHelper(store);
            
            URI S = new URIImpl("http://www.bigdata.com/s");
            URI P = new URIImpl("http://www.bigdata.com/p");
            URI O = new URIImpl("http://www.bigdata.com/o");
            
            long s = store.addTerm(S);
            long p = store.addTerm(P);
            long o = store.addTerm(O);

            SPOAssertionBuffer buffer = new SPOAssertionBuffer(store, store,
                    null/* filter */, 100/* capacity */, false/* justified */);

            buffer.add(new SPO(s, p, o, StatementEnum.Explicit));

            buffer.flush();

            AbstractTestCase.assertSameSPOs(new SPO[] {
                    new SPO(s, p, o, StatementEnum.Explicit),
                    },
                    store.getAccessPath(NULL, NULL, NULL).iterator()
                    );

            {
                // where s is bound.
                ISPOIterator itr = new BackchainTypeResourceIterator(//
                        store.getAccessPath(s, vocab.rdfType.id,
                                vocab.rdfsResource.id).iterator(),//
                        s, vocab.rdfType.id, vocab.rdfsResource.id,//
                        store, //
                        vocab.rdfType.id, //
                        vocab.rdfsResource.id //
                );

                AbstractTestCase.assertSameSPOs(new SPO[] { new SPO(s,
                        vocab.rdfType.id, vocab.rdfsResource.id,
                        StatementEnum.Inferred), }, itr);
            }

            {
                // where s is unbound.
                ISPOIterator itr = new BackchainTypeResourceIterator(//
                        store.getAccessPath(NULL, vocab.rdfType.id,
                                vocab.rdfsResource.id).iterator(),//
                        NULL, vocab.rdfType.id, vocab.rdfsResource.id,//
                        store, //
                        vocab.rdfType.id, //
                        vocab.rdfsResource.id //
                );

                AbstractTestCase.assertSameSPOs(new SPO[] { new SPO(s,
                        vocab.rdfType.id, vocab.rdfsResource.id,
                        StatementEnum.Inferred), }, itr);
            }
            
        } finally {

            store.closeAndDelete();
            
        }
            
    }

    /**
     * @todo write a test where we compute the forward closure of a data set
     *       with (x type Resource) entailments included in the rule set and
     *       then compare it to the forward closure of the same data set
     *       computed without those entailments and using backward chaining to
     *       supply the entailments. The result graphs should be equals.
     */
    public void test_forwardClosure() {
        
        fail("write test");
        
    }
    
}
