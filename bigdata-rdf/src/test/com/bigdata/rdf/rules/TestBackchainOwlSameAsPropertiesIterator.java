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
 * Created on March 11, 2008
 */

package com.bigdata.rdf.rules;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import com.bigdata.rdf.inf.BackchainOwlSameAsPropertiesIterator;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOAccessPath;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Test suite for {@link BackchainOwlSameAsPropertiesIterator}.
 * 
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestBackchainOwlSameAsPropertiesIterator extends AbstractInferenceEngineTestCase {

    /**
     * 
     */
    public TestBackchainOwlSameAsPropertiesIterator() {
        super();
    }

    /**
     * @param name
     */
    public TestBackchainOwlSameAsPropertiesIterator(String name) {
        super(name);
    }

//    final TemporaryStore tempStore = new TemporaryStore();
    
    /**
     * Test the various access paths for backchaining the property collection
     * normally done through owl:sameAs {2,3}.
     */
    public void test_backchain() 
    {
     
        // store with no owl:sameAs closure
        final AbstractTripleStore noClosure = getStore();
        
        try {

            final URI A = new URIImpl("http://www.bigdata.com/A");
            final URI B = new URIImpl("http://www.bigdata.com/B");
            final URI W = new URIImpl("http://www.bigdata.com/W");
            final URI X = new URIImpl("http://www.bigdata.com/X");
            final URI Y = new URIImpl("http://www.bigdata.com/Y");
            final URI Z = new URIImpl("http://www.bigdata.com/Z");

            {
                StatementBuffer buffer = new StatementBuffer
                    ( noClosure, 100/* capacity */
                      );

                buffer.add(X, A, Z);
                buffer.add(Y, B, W);
                buffer.add(X, OWL.SAMEAS, Y);
                buffer.add(Z, OWL.SAMEAS, W);
                
                // write statements on the database.
                buffer.flush();
                
                // database at once closure.
                noClosure.getInferenceEngine()
                        .computeClosure(null/*focusStore*/);

            }
            
            final IV a = noClosure.getIV(A);
            final IV b = noClosure.getIV(B);
            final IV w = noClosure.getIV(W);
            final IV x = noClosure.getIV(X);
            final IV y = noClosure.getIV(Y);
            final IV z = noClosure.getIV(Z);
            final IV same = noClosure.getIV(OWL.SAMEAS);
            final IV type = noClosure.getIV(RDF.TYPE);
            final IV property = noClosure.getIV(RDF.PROPERTY);
            final IV subpropof = noClosure.getIV(RDFS.SUBPROPERTYOF);
            
            if (log.isInfoEnabled())
                log.info("\n" +noClosure.dumpStore(true, true, false));
  
            { // test S
            
                SPOAccessPath accessPath = (SPOAccessPath)noClosure.getAccessPath(y,null,null);
                
                IChunkedOrderedIterator<ISPO> itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.get(0/*S*/),
                        accessPath.get(1/*P*/),
                        accessPath.get(2/*O*/),
                        noClosure, //
                        same //
                        );

                assertSameSPOsAnyOrder(noClosure,
                    
                    new SPO[]{
                        new SPO(y,b,w,
                                StatementEnum.Explicit),
                        new SPO(y,b,z,
                                StatementEnum.Inferred),
                        new SPO(y,a,w,
                                StatementEnum.Inferred),
                        new SPO(y,a,z,
                                StatementEnum.Inferred),
                        new SPO(y,same,x,
                                StatementEnum.Inferred)
                    },
                    
                    itr,
                    true // ignore axioms
                    
                );
                
            }
          
            { // test SP
                
                SPOAccessPath accessPath = (SPOAccessPath)noClosure.getAccessPath(y,b,null);
                
                IChunkedOrderedIterator<ISPO> itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.get(0/*S*/),
                        accessPath.get(1/*P*/),
                        accessPath.get(2/*O*/),
                        noClosure, //
                        same //
                        );

                assertSameSPOsAnyOrder(noClosure,
                    
                    new SPO[]{
                        new SPO(y,b,w,
                                StatementEnum.Explicit),
                        new SPO(y,b,z,
                                StatementEnum.Inferred)
                    },
                    
                    itr,
                    true // ignore axioms
                    
                );
                
            }
          
            { // test O
                
                SPOAccessPath accessPath = (SPOAccessPath)noClosure.getAccessPath(null,null,w);
                
                IChunkedOrderedIterator<ISPO> itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.get(0/*S*/),
                        accessPath.get(1/*P*/),
                        accessPath.get(2/*O*/),
                        noClosure, //
                        same //
                        );

                assertSameSPOsAnyOrder(noClosure,
                    
                    new SPO[]{
                        new SPO(y,b,w,
                                StatementEnum.Explicit),
                        new SPO(y,a,w,
                                StatementEnum.Inferred),
                        new SPO(x,b,w,
                                StatementEnum.Inferred),
                        new SPO(x,a,w,
                                StatementEnum.Inferred),
                        new SPO(z,same,w,
                                StatementEnum.Explicit)
                    },
                    
                    itr,
                    true // ignore axioms
                    
                );
                
            }
          
            { // test PO
                
                SPOAccessPath accessPath = (SPOAccessPath)noClosure.getAccessPath(null,a,w);
                
                IChunkedOrderedIterator<ISPO> itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.get(0/*S*/),
                        accessPath.get(1/*P*/),
                        accessPath.get(2/*O*/),
                        noClosure, //
                        same //
                        );

                assertSameSPOsAnyOrder(noClosure,
                    
                    new SPO[]{
                        new SPO(y,a,w,
                                StatementEnum.Inferred),
                        new SPO(x,a,w,
                                StatementEnum.Inferred)
                    },
                    
                    itr,
                    true // ignore axioms
                    
                );
                
            }
          
            { // test SO
                
                SPOAccessPath accessPath = (SPOAccessPath)noClosure.getAccessPath(x,null,z);
                
                IChunkedOrderedIterator<ISPO> itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.get(0/*S*/),
                        accessPath.get(1/*P*/),
                        accessPath.get(2/*O*/),
                        noClosure, //
                        same //
                        );

                assertSameSPOsAnyOrder(noClosure,
                    
                    new SPO[]{
                        new SPO(x,a,z,
                                StatementEnum.Explicit),
                        new SPO(x,b,z,
                                StatementEnum.Inferred)
                    },
                    
                    itr,
                    true // ignore axioms
                    
                );
                
            }
          
            { // test SPO
                
                SPOAccessPath accessPath = (SPOAccessPath)noClosure.getAccessPath(x,b,z);
                
                IChunkedOrderedIterator<ISPO> itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.get(0/*S*/),
                        accessPath.get(1/*P*/),
                        accessPath.get(2/*O*/),
                        noClosure, //
                        same //
                        );

                assertSameSPOsAnyOrder(noClosure,
                    
                    new SPO[]{
                        new SPO(x,b,z,
                                StatementEnum.Inferred)
                    },
                    
                    itr,
                    true // ignore axioms
                    
                );
                
            }
          
            { // test P
                
                SPOAccessPath accessPath = (SPOAccessPath)noClosure.getAccessPath(null,a,null);
                
                IChunkedOrderedIterator<ISPO> itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.get(0/*S*/),
                        accessPath.get(1/*P*/),
                        accessPath.get(2/*O*/),
                        noClosure, //
                        same //
                        );

                assertSameSPOsAnyOrder(noClosure,
                    
                    new SPO[]{
                        new SPO(x,a,z,
                                StatementEnum.Explicit),
                        new SPO(x,a,w,
                                StatementEnum.Inferred),
                        new SPO(y,a,z,
                                StatementEnum.Inferred),
                        new SPO(y,a,w,
                                StatementEnum.Inferred)
                    },
                    
                    itr,
                    true // ignore axioms
                    
                );
                
            }
          
            { // test ???
                
                SPOAccessPath accessPath = (SPOAccessPath)noClosure.getAccessPath(NULL,NULL,NULL);
                
                IChunkedOrderedIterator<ISPO> itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.get(0/*S*/),
                        accessPath.get(1/*P*/),
                        accessPath.get(2/*O*/),
                        noClosure, //
                        same //
                        );

                assertSameSPOsAnyOrder(noClosure,
                    
                    new SPO[]{
                        new SPO(x,a,z,
                                StatementEnum.Explicit),
                        new SPO(y,b,w,
                                StatementEnum.Explicit),
                        new SPO(x,same,y,
                                StatementEnum.Explicit),
                        new SPO(z,same,w,
                                StatementEnum.Explicit),
                        new SPO(x,a,w,
                                StatementEnum.Inferred),
                        new SPO(x,b,z,
                                StatementEnum.Inferred),
                        new SPO(x,b,w,
                                StatementEnum.Inferred),
                        new SPO(y,a,z,
                                StatementEnum.Inferred),
                        new SPO(y,a,w,
                                StatementEnum.Inferred),
                        new SPO(y,b,z,
                                StatementEnum.Inferred),
                        new SPO(y,same,x,
                                StatementEnum.Inferred),
                        new SPO(w,same,z,
                                StatementEnum.Inferred),
                        new SPO(a,type,property,
                                StatementEnum.Inferred),
                        new SPO(b,type,property,
                                StatementEnum.Inferred),
                        new SPO(a,subpropof,a,
                                StatementEnum.Inferred),
                        new SPO(b,subpropof,b,
                                StatementEnum.Inferred)
                    },
                    
                    itr,
                    true // ignore axioms
                    
                );
                
            }
          
        } finally {
            
            noClosure.__tearDownUnitTest();
            
        }
        
    }

}
