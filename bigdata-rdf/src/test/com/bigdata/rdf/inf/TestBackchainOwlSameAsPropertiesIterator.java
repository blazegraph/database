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

package com.bigdata.rdf.inf;

import java.util.Properties;
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
import com.bigdata.rdf.store.TempTripleStore;

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

    /**
     * Test the various access paths for backchaining the property collection
     * normally done through owl:sameAs {2,3}.
     */
    public void test_backchain() 
    {
     
        // store with no owl:sameAs closure
        AbstractTripleStore noClosure = getStore();
        
        try {

            InferenceEngine inf = noClosure.getInferenceEngine();
            
            Rule[] rules = inf.getRuleModel();
            for( Rule rule : rules ) {
                System.err.println(rule.getName());
            }
            
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
            
            final long a = noClosure.getTermId(A);
            final long b = noClosure.getTermId(B);
            final long c = noClosure.getTermId(C);
            final long d = noClosure.getTermId(D);
            final long e = noClosure.getTermId(E);
            final long v = noClosure.getTermId(V);
            final long w = noClosure.getTermId(W);
            final long x = noClosure.getTermId(X);
            final long y = noClosure.getTermId(Y);
            final long z = noClosure.getTermId(Z);
            final long same = noClosure.getTermId(OWL.SAMEAS);
            final long type = noClosure.getTermId(RDF.TYPE);
            final long property = noClosure.getTermId(RDF.PROPERTY);
            final long subpropof = noClosure.getTermId(RDFS.SUBPROPERTYOF);
            
            noClosure.dumpStore(true, true, false);
  
            { // test S
            
                IAccessPath accessPath = noClosure.getAccessPath(y,NULL,NULL);
                
                ISPOIterator itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.getTriplePattern()[0],
                        accessPath.getTriplePattern()[1],
                        accessPath.getTriplePattern()[2],
                        noClosure, //
                        same
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
                
                IAccessPath accessPath = noClosure.getAccessPath(y,b,NULL);
                
                ISPOIterator itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.getTriplePattern()[0],
                        accessPath.getTriplePattern()[1],
                        accessPath.getTriplePattern()[2],
                        noClosure, //
                        same
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
                
                IAccessPath accessPath = noClosure.getAccessPath(NULL,NULL,w);
                
                ISPOIterator itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.getTriplePattern()[0],
                        accessPath.getTriplePattern()[1],
                        accessPath.getTriplePattern()[2],
                        noClosure, //
                        same
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
                
                IAccessPath accessPath = noClosure.getAccessPath(NULL,a,w);
                
                ISPOIterator itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.getTriplePattern()[0],
                        accessPath.getTriplePattern()[1],
                        accessPath.getTriplePattern()[2],
                        noClosure, //
                        same
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
                
                IAccessPath accessPath = noClosure.getAccessPath(x,NULL,z);
                
                ISPOIterator itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.getTriplePattern()[0],
                        accessPath.getTriplePattern()[1],
                        accessPath.getTriplePattern()[2],
                        noClosure, //
                        same
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
                
                IAccessPath accessPath = noClosure.getAccessPath(x,b,z);
                
                ISPOIterator itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.getTriplePattern()[0],
                        accessPath.getTriplePattern()[1],
                        accessPath.getTriplePattern()[2],
                        noClosure, //
                        same
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
                
                IAccessPath accessPath = noClosure.getAccessPath(NULL,a,NULL);
                
                ISPOIterator itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.getTriplePattern()[0],
                        accessPath.getTriplePattern()[1],
                        accessPath.getTriplePattern()[2],
                        noClosure, //
                        same
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
                
                IAccessPath accessPath = noClosure.getAccessPath(NULL,NULL,NULL);
                
                ISPOIterator itr = new BackchainOwlSameAsPropertiesIterator(//
                        accessPath.iterator(),//
                        accessPath.getTriplePattern()[0],
                        accessPath.getTriplePattern()[1],
                        accessPath.getTriplePattern()[2],
                        noClosure, //
                        same
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
            
            noClosure.closeAndDelete();
            
        }
        
    }

}
