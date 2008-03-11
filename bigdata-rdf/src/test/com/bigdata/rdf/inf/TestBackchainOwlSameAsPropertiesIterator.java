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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.inf.TMStatementBuffer.BufferEnum;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IAccessPath;

/**
 * Test suite for {@link BackchainTypeResourceIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBackchainOwlSameAsPropertiesIterator extends AbstractInferenceEngineTestCase {

    static {
        Logger.getLogger("com.bigdata").setLevel(Level.ERROR);
    }
    
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

    /**
     * Test the various access paths for backchaining the property collection
     * normally done through owl:sameAs {2,3}.
     */
    public void test_backchain() 
    {
     
        AbstractTripleStore store = getStore();

        try {

            InferenceEngine inf = store.getInferenceEngine();
            
            
            
            final URI A = new URIImpl("http://www.bigdata.com/A");
            final URI B = new URIImpl("http://www.bigdata.com/B");
            final URI C = new URIImpl("http://www.bigdata.com/C");
            final URI D = new URIImpl("http://www.bigdata.com/D");
            final URI E = new URIImpl("http://www.bigdata.com/E");

            final URI V = new URIImpl("http://www.bigdata.com/V");
            final URI W = new URIImpl("http://www.bigdata.com/W");
            final URI X = new URIImpl("http://www.bigdata.com/X");
            final URI Y = new URIImpl("http://www.bigdata.com/Y");
            final URI Z = new URIImpl("http://www.bigdata.com/Z");

            {
                TMStatementBuffer buffer = new TMStatementBuffer
                    ( inf, 100/* capacity */, BufferEnum.AssertionBuffer
                      );

                buffer.add(X, A, Z);
                buffer.add(Y, B, W);
                buffer.add(X, OWL.SAMEAS, Y);
                buffer.add(Z, OWL.SAMEAS, W);
                buffer.doClosure();

                // write on the store.
//                buffer.flush();
            }
            
            final long a = store.getTermId(A);
            final long b = store.getTermId(B);
            final long c = store.getTermId(C);
            final long d = store.getTermId(D);
            final long e = store.getTermId(E);
            final long v = store.getTermId(V);
            final long w = store.getTermId(W);
            final long x = store.getTermId(X);
            final long y = store.getTermId(Y);
            final long z = store.getTermId(Z);
            final long same = store.getTermId(OWL.SAMEAS);
            final long type = store.getTermId(RDF.TYPE);
            final long property = store.getTermId(RDF.PROPERTY);
            final long subpropof = store.getTermId(RDFS.SUBPROPERTYOF);
            
            store.dumpStore(true, true, false);
  
            { // test S
            
                IAccessPath accessPath = store.getAccessPath(y,NULL,NULL);
                
                ISPOIterator itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.getTriplePattern()[0],
                        accessPath.getTriplePattern()[1],
                        accessPath.getTriplePattern()[2],
                        store, //
                        same
                        );

                assertSameSPOsAnyOrder(store,
                    
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
                    
                    itr
                    
                );
                
            }
          
            { // test SP
                
                IAccessPath accessPath = store.getAccessPath(y,b,NULL);
                
                ISPOIterator itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.getTriplePattern()[0],
                        accessPath.getTriplePattern()[1],
                        accessPath.getTriplePattern()[2],
                        store, //
                        same
                        );

                assertSameSPOsAnyOrder(store,
                    
                    new SPO[]{
                        new SPO(y,b,w,
                                StatementEnum.Explicit),
                        new SPO(y,b,z,
                                StatementEnum.Inferred)
                    },
                    
                    itr
                    
                );
                
            }
          
            { // test O
                
                IAccessPath accessPath = store.getAccessPath(NULL,NULL,w);
                
                ISPOIterator itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.getTriplePattern()[0],
                        accessPath.getTriplePattern()[1],
                        accessPath.getTriplePattern()[2],
                        store, //
                        same
                        );

                assertSameSPOsAnyOrder(store,
                    
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
                    
                    itr
                    
                );
                
            }
          
            { // test PO
                
                IAccessPath accessPath = store.getAccessPath(NULL,a,w);
                
                ISPOIterator itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.getTriplePattern()[0],
                        accessPath.getTriplePattern()[1],
                        accessPath.getTriplePattern()[2],
                        store, //
                        same
                        );

                assertSameSPOsAnyOrder(store,
                    
                    new SPO[]{
                        new SPO(y,a,w,
                                StatementEnum.Inferred),
                        new SPO(x,a,w,
                                StatementEnum.Inferred)
                    },
                    
                    itr
                    
                );
                
            }
          
            { // test SO
                
                IAccessPath accessPath = store.getAccessPath(x,NULL,z);
                
                ISPOIterator itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.getTriplePattern()[0],
                        accessPath.getTriplePattern()[1],
                        accessPath.getTriplePattern()[2],
                        store, //
                        same
                        );

                assertSameSPOsAnyOrder(store,
                    
                    new SPO[]{
                        new SPO(x,a,z,
                                StatementEnum.Explicit),
                        new SPO(x,b,z,
                                StatementEnum.Inferred)
                    },
                    
                    itr,
                    true /*ignore the axioms*/
                    
                );
                
            }
          
            { // test SPO
                
                IAccessPath accessPath = store.getAccessPath(x,b,z);
                
                ISPOIterator itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.getTriplePattern()[0],
                        accessPath.getTriplePattern()[1],
                        accessPath.getTriplePattern()[2],
                        store, //
                        same
                        );

                assertSameSPOsAnyOrder(store,
                    
                    new SPO[]{
                        new SPO(x,b,z,
                                StatementEnum.Inferred)
                    },
                    
                    itr,
                    true /*ignore the axioms*/
                    
                );
                
            }
          
            { // test P
                
                IAccessPath accessPath = store.getAccessPath(NULL,a,NULL);
                
                ISPOIterator itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.getTriplePattern()[0],
                        accessPath.getTriplePattern()[1],
                        accessPath.getTriplePattern()[2],
                        store, //
                        same
                        );

                assertSameSPOsAnyOrder(store,
                    
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
                    true /*ignore the axioms*/
                    
                );
                
            }
          
            { // test ???
                
                IAccessPath accessPath = store.getAccessPath(NULL,NULL,NULL);
                
                ISPOIterator itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.getTriplePattern()[0],
                        accessPath.getTriplePattern()[1],
                        accessPath.getTriplePattern()[2],
                        store, //
                        same
                        );

                assertSameSPOsAnyOrder(store,
                    
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
                    true /*ignore the axioms*/
                    
                );
                
            }
          
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }

}
