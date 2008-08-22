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

package com.bigdata.rdf.rules;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.inf.BackchainTypeResourceIterator;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Test suite for {@link BackchainTypeResourceIterator}.
 * 
 * @todo write a test where we compute the forward closure of a data set with (x
 *       type Resource) entailments included in the rule set and then compare it
 *       to the forward closure of the same data set computed without those
 *       entailments and using backward chaining to supply the entailments. The
 *       result graphs should be equals.
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
            final RDFSVocabulary vocab = new RDFSVocabulary(store);

            final BigdataValueFactory f = store.getValueFactory();
            
            final BigdataURI A = f.createURI("http://www.foo.org/A");
            final BigdataURI B = f.createURI("http://www.foo.org/B");
            final BigdataURI C = f.createURI("http://www.foo.org/C");

            /*
             * add statements to the store.
             * 
             * Note: this gives us TWO (2) distinct subjects (A and B), but we 
             * will only visit statements for (A).
             */
            {
                
                final IStatementBuffer buffer = new StatementBuffer(store, 10/* capacity */);

                buffer.add(A, RDF.TYPE, B);
                buffer.add(B, RDF.TYPE, C);

                buffer.flush();
                
            }
            
            final IChunkedOrderedIterator<ISPO> itr;
            {

                final IAccessPath<ISPO> accessPath = store.getAccessPath(store
                        .getTermId(A), NULL, NULL);
             
                itr = BackchainTypeResourceIterator.newInstance(//
                    accessPath.iterator(),//
                    accessPath,//
                    store, //
                    vocab.rdfType.get(), //
                    vocab.rdfsResource.get()//
                    );
                
            }
            
            assertSameSPOsAnyOrder(store,
                    
                    new SPO[]{
                    
                    new SPO(//
                            store.getTermId(A),//
                            store.getTermId(RDF.TYPE),//
                            store.getTermId(B),//
                            StatementEnum.Explicit),
                            
                    new SPO(//
                            store.getTermId(A), //
                            store.getTermId(RDF.TYPE), //
                            store.getTermId(RDFS.RESOURCE), //
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
            final RDFSVocabulary vocab = new RDFSVocabulary(store);

            final BigdataValueFactory f = store.getValueFactory();
            
            final BigdataURI A = f.createURI("http://www.foo.org/A");
            final BigdataURI B = f.createURI("http://www.foo.org/B");
            final BigdataURI C = f.createURI("http://www.foo.org/C");

            /*
             * add statements to the store.
             * 
             * Note: this gives us TWO (2) distinct subjects (A and B), but we
             * will only visit statements for (A).
             */
            {
                
                IStatementBuffer buffer = new StatementBuffer(store, 10/* capacity */);

                buffer.add(A, RDF.TYPE, B);

                buffer.add(A, RDF.TYPE, RDFS.RESOURCE);

                buffer.add(B, RDF.TYPE, C);

                buffer.flush();
                
            }

            if(log.isInfoEnabled()) log.info("\n"+store.dumpStore());

            final IChunkedOrderedIterator<ISPO> itr;
            {

                final IAccessPath<ISPO> accessPath = store.getAccessPath(store
                        .getTermId(A), NULL, NULL);

                itr = BackchainTypeResourceIterator.newInstance(//
                        accessPath.iterator(),//
                        accessPath,//
                        store, //
                        vocab.rdfType.get(), //
                        vocab.rdfsResource.get() //
                        );
                
            }

            assertSameSPOsAnyOrder(store, new SPO[]{
                    
                    new SPO(//
                            store.getTermId(A),//
                            store.getTermId(RDF.TYPE),//
                            store.getTermId(B),//
                            StatementEnum.Explicit),
                            
                    new SPO(//
                            store.getTermId(A), //
                            store.getTermId(RDF.TYPE), //
                            store.getTermId(RDFS.RESOURCE), //
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
        
        final AbstractTripleStore store = getStore();
        
        try {

            // adds rdf:Type and rdfs:Resource to the store.
            final RDFSVocabulary vocab = new RDFSVocabulary(store);

            final BigdataValueFactory f = store.getValueFactory();
            
            final BigdataURI A = f.createURI("http://www.foo.org/A");
            final BigdataURI B = f.createURI("http://www.foo.org/B");
            final BigdataURI C = f.createURI("http://www.foo.org/C");

            /*
             * add statements to the store.
             * 
             * Note: this gives us TWO (2) distinct subjects (A and B). Since
             * nothing is bound in the query we will visit both explicit
             * statements and also the (s type resource) entailments for both
             * distinct subjects.
             */
            {
                
                final IStatementBuffer buffer = new StatementBuffer(store, 10/* capacity */);

                buffer.add(A, RDF.TYPE, B);

                buffer.add(A, RDF.TYPE, RDFS.RESOURCE);

                buffer.add(B, RDF.TYPE, C);

                buffer.flush();
                
            }

            final IChunkedOrderedIterator<ISPO> itr;
            {
                final IAccessPath<ISPO> accessPath = store.getAccessPath(NULL,
                        NULL, NULL);

                itr = BackchainTypeResourceIterator.newInstance(//
                        accessPath.iterator(),//
                        accessPath,//
                        store, //
                        vocab.rdfType.get(), //
                        vocab.rdfsResource.get() //
                );

            }
            
            assertSameSPOsAnyOrder(store, new SPO[]{

                    new SPO(//
                            store.getTermId(A),//
                            store.getTermId(RDF.TYPE),//
                            store.getTermId(B),//
                            StatementEnum.Explicit),

                    new SPO(//
                            store.getTermId(B),//
                            store.getTermId(RDF.TYPE),//
                            store.getTermId(C),//
                            StatementEnum.Explicit),
                    
                    new SPO(//
                            store.getTermId(A), //
                            store.getTermId(RDF.TYPE), //
                            store.getTermId(RDFS.RESOURCE), //
                            StatementEnum.Explicit),
                    
                    new SPO(//
                            store.getTermId(B), //
                            store.getTermId(RDF.TYPE), //
                            store.getTermId(RDFS.RESOURCE), //
                            StatementEnum.Inferred),
                    
                    new SPO(//
                            store.getTermId(C), //
                            store.getTermId(RDF.TYPE), //
                            store.getTermId(RDFS.RESOURCE), //
                            StatementEnum.Inferred),
                    
                    new SPO(//
                            store.getTermId(RDFS.RESOURCE), //
                            store.getTermId(RDF.TYPE), //
                            store.getTermId(RDFS.RESOURCE), //
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
        
        final AbstractTripleStore store = getStore();
        
        try {

            // adds rdf:Type and rdfs:Resource to the store.
            final RDFSVocabulary vocab = new RDFSVocabulary(store);

            final BigdataValueFactory f = store.getValueFactory();
            
            final BigdataURI A = f.createURI("http://www.foo.org/A");
            final BigdataURI B = f.createURI("http://www.foo.org/B");
            final BigdataURI C = f.createURI("http://www.foo.org/C");

            /*
             * add statements to the store.
             * 
             * Note: this gives us TWO (2) distinct subjects (A and B).
             */
            {
                
                IStatementBuffer buffer = new StatementBuffer(store, 10/* capacity */);

                buffer.add(A, RDF.TYPE, B);
                buffer.add(B, RDF.TYPE, C);

                buffer.flush();
                
            }

            final IChunkedOrderedIterator<ISPO> itr;
            {
                final IAccessPath<ISPO> accessPath = store.getAccessPath(NULL,
                        NULL, store.getTermId(B));

                itr = BackchainTypeResourceIterator.newInstance(//
                        accessPath.iterator(),//
                        accessPath,//
                        store, //
                        vocab.rdfType.get(), //
                        vocab.rdfsResource.get() //
                        );
                
            }

            /*
             * Note: Since we are reading with the object bound, only the
             * explicit statements for that object should make it into the
             * iterator.
             */

            assertSameSPOsAnyOrder(store, new SPO[]{
                    
                    new SPO(//
                            store.getTermId(A),//
                            store.getTermId(RDF.TYPE),//
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

        final AbstractTripleStore store = getStore();

        try {

            final RDFSVocabulary vocab = new RDFSVocabulary(store);
            
            final BigdataValueFactory f = store.getValueFactory();
            
            final BigdataURI S = f.createURI("http://www.bigdata.com/s");
            final BigdataURI P = f.createURI("http://www.bigdata.com/p");
            final BigdataURI O = f.createURI("http://www.bigdata.com/o");
            
            final long s = store.addTerm(S);
            final long p = store.addTerm(P);
            final long o = store.addTerm(O);

            store.addStatements(new SPO[] {//
                    new SPO(s, p, o, StatementEnum.Explicit) //
                    }, 1);

            if(log.isInfoEnabled()) {
                log.info("\n:"+store.dumpStore());
            }
            
            AbstractTestCase.assertSameSPOs(new SPO[] {
                    new SPO(s, p, o, StatementEnum.Explicit),
                    },
                    store.getAccessPath(NULL, NULL, NULL).iterator()
                    );

            {
                // where s is bound.
                final IAccessPath<ISPO> accessPath = store.getAccessPath(s,
                        vocab.rdfType.get(), vocab.rdfsResource.get());

                final IChunkedOrderedIterator<ISPO> itr = BackchainTypeResourceIterator
                        .newInstance(//
                                accessPath.iterator(),//
                                accessPath,//
                                store, //
                                vocab.rdfType.get(), //
                                vocab.rdfsResource.get() //
                        );

                AbstractTestCase.assertSameSPOs(new SPO[] { new SPO(s,
                        vocab.rdfType.get(), vocab.rdfsResource.get(),
                        StatementEnum.Inferred), }, itr);
            }

            {
                // where s is unbound.
                final IAccessPath<ISPO> accessPath = store.getAccessPath(NULL,
                        vocab.rdfType.get(), vocab.rdfsResource.get());
                
                final IChunkedOrderedIterator<ISPO> itr = BackchainTypeResourceIterator.newInstance(//
                        accessPath.iterator(),//
                        accessPath,//
                        store, //
                        vocab.rdfType.get(), //
                        vocab.rdfsResource.get() //
                );

                AbstractTestCase.assertSameSPOs(new SPO[] { //
                        new SPO(s, vocab.rdfType.get(), vocab.rdfsResource
                                .get(), StatementEnum.Inferred), //
                        new SPO(o, vocab.rdfType.get(), vocab.rdfsResource
                                .get(), StatementEnum.Inferred), //
                        }, itr);
            }
            
        } finally {

            store.closeAndDelete();
            
        }
            
    }

}
