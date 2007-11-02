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
 * Created on Oct 30, 2007
 */

package com.bigdata.rdf.inf;

import java.util.Map;
import java.util.TreeMap;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOComparator;
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
     * the iterator MUST add a single entailment (s rdf:Type rdfs:Resource).
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

            /*
             * build up the expected SPOs.
             */
            Map<SPO,SPO> expected = new TreeMap<SPO,SPO>(SPOComparator.INSTANCE);
            
            {
                SPO spo = new SPO(//
                        store.getTermId(A),//
                        store.getTermId(URIImpl.RDF_TYPE),//
                        store.getTermId(B),//
                        StatementEnum.Explicit);
                
                expected.put(spo,spo);
            }

            {
                SPO spo = new SPO(//
                        store.getTermId(A), //
                        store.getTermId(URIImpl.RDF_TYPE), //
                        store.getTermId(URIImpl.RDFS_RESOURCE), //
                        StatementEnum.Inferred);
                
                expected.put(spo,spo);
            }

            ISPOIterator itr = new BackchainTypeResourceIterator(//
                    store.getAccessPath(store.getTermId(A), NULL, NULL).iterator(),//
                    store.getTermId(A), NULL, NULL,//
                    store, //
                    vocab.rdfType.id, //
                    vocab.rdfsResource.id //
                    );

            int i = 0;
            
            while(itr.hasNext()) {
                
                SPO actualSPO = itr.next();
                
                System.err.println("actual: "+actualSPO.toString(store));
                
                SPO expectedSPO = expected.remove(actualSPO);

                if(expectedSPO==null) {
                    
                    fail("Not expecting: "+actualSPO.toString(store)+" at index="+i);
                    
                }

                System.err.println("expected: "+expectedSPO.toString(store));

                assertEquals(expectedSPO.type, actualSPO.type);
                
                i++;
                
            }
            
            if(!expected.isEmpty()) {
                
                fail("Expecting: "+expected.size()+" more statements");
                
            }
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }

    /**
     * Test when the triple pattern has no bound variables. In this case the
     * iterator MUST add an entailment for each distinct subject in the store.
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
            buffer.add(B, URIImpl.RDF_TYPE, C);
            
            buffer.flush();

            /*
             * build up the expected SPOs.
             */
            Map<SPO,SPO> expected = new TreeMap<SPO,SPO>(SPOComparator.INSTANCE);
            
            {
                SPO spo = new SPO(//
                        store.getTermId(A),//
                        store.getTermId(URIImpl.RDF_TYPE),//
                        store.getTermId(B),//
                        StatementEnum.Explicit);
                
                expected.put(spo,spo);
            }

            {
                SPO spo = new SPO(//
                        store.getTermId(B),//
                        store.getTermId(URIImpl.RDF_TYPE),//
                        store.getTermId(C),//
                        StatementEnum.Explicit);
                
                expected.put(spo,spo);
            }

            {
                SPO spo = new SPO(//
                        store.getTermId(A), //
                        store.getTermId(URIImpl.RDF_TYPE), //
                        store.getTermId(URIImpl.RDFS_RESOURCE), //
                        StatementEnum.Inferred);
                
                expected.put(spo,spo);
            }

            {
                SPO spo = new SPO(//
                        store.getTermId(B), //
                        store.getTermId(URIImpl.RDF_TYPE), //
                        store.getTermId(URIImpl.RDFS_RESOURCE), //
                        StatementEnum.Inferred);
                
                expected.put(spo,spo);
            }

            ISPOIterator itr = new BackchainTypeResourceIterator(//
                    store.getAccessPath(NULL, NULL, NULL).iterator(),//
                    NULL, NULL, NULL,//
                    store, //
                    vocab.rdfType.id, //
                    vocab.rdfsResource.id //
                    );

            int i = 0;
            
            while(itr.hasNext()) {
                
                SPO actualSPO = itr.next();
                
                System.err.println("actual: "+actualSPO.toString(store));
                
                SPO expectedSPO = expected.remove(actualSPO);

                if(expectedSPO==null) {
                    
                    fail("Not expecting: "+actualSPO.toString(store)+" at index="+i);
                    
                }

                System.err.println("expected: "+expectedSPO.toString(store));

                assertEquals(expectedSPO.type, actualSPO.type);
                
                i++;
                
            }
            
            if(!expected.isEmpty()) {
                
                fail("Expecting: "+expected.size()+" more statements");
                
            }
            
        } finally {
            
            store.closeAndDelete();
            
        }

    }
    
    /**
     * Test for other triple patterns (all bound, predicate bound, object bound,
     * etc). In all cases the iterator MUST NOT add any entailments.
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

            /*
             * build up the expected SPOs. Since we are reading with the object
             * bound, only the explicit statements for that object should make
             * it into the iterator.
             */
            Map<SPO,SPO> expected = new TreeMap<SPO,SPO>(SPOComparator.INSTANCE);
            
            {
                SPO spo = new SPO(//
                        store.getTermId(A),//
                        store.getTermId(URIImpl.RDF_TYPE),//
                        store.getTermId(B),//
                        StatementEnum.Explicit);
                
                expected.put(spo,spo);
            }

            ISPOIterator itr = new BackchainTypeResourceIterator(//
                    store.getAccessPath(NULL, NULL, store.getTermId(B)).iterator(),//
                    NULL, NULL, store.getTermId(B),//
                    store, //
                    vocab.rdfType.id, //
                    vocab.rdfsResource.id //
                    );

            int i = 0;
            
            while(itr.hasNext()) {
                
                SPO actualSPO = itr.next();
                
                System.err.println("actual: "+actualSPO.toString(store));
                
                SPO expectedSPO = expected.remove(actualSPO);

                if(expectedSPO==null) {
                    
                    fail("Not expecting: "+actualSPO.toString(store)+" at index="+i);
                    
                }

                System.err.println("expected: "+expectedSPO.toString(store));

                assertEquals(expectedSPO.type, actualSPO.type);
                
                i++;
                
            }
            
            if(!expected.isEmpty()) {
                
                fail("Expecting: "+expected.size()+" more statements");
                
            }
            
        } finally {
            
            store.closeAndDelete();
            
        }

    }
    
}
