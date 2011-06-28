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
 * Created on March 19, 2008
 */

package com.bigdata.rdf.store;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Test bulk filter API.
 * 
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestBulkFilter extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestBulkFilter() {
    }

    /**
     * @param name
     */
    public TestBulkFilter(String name) {
        super(name);
    }

    /**
     * Simple test of bulk filtering out and in.
     */
    public void test_filterOut() {

        AbstractTripleStore store = getStore();
        
        try {

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
            
            store.addTerm(A);
            store.addTerm(B);
            store.addTerm(C);
            store.addTerm(D);
            store.addTerm(E);
            
            store.addTerm(V);
            store.addTerm(W);
            store.addTerm(X);
            store.addTerm(Y);
            store.addTerm(Z);
            
            store.addStatement(X, A, Y);
            store.addStatement(X, A, Z);
            
//            store.commit();
    
            final IV a = store.getIV(A);
//            final IV b = store.getTermId(B);
//            final IV c = store.getTermId(C);
//            final IV d = store.getTermId(D);
//            final IV e = store.getTermId(E);
            final IV v = store.getIV(V);
            final IV w = store.getIV(W);
            final IV x = store.getIV(X);
            final IV y = store.getIV(Y);
            final IV z = store.getIV(Z);
            
            if (log.isInfoEnabled())
                log.info("\n" + store.dumpStore(true, true, false));

            SPO[] stmts = new SPO[] {
                new SPO(x,a,y,StatementEnum.Explicit),
                new SPO(x,a,z,StatementEnum.Explicit),
                new SPO(x,a,v,StatementEnum.Explicit),
                new SPO(x,a,w,StatementEnum.Explicit)
            };
            int numStmts = stmts.length;

            { // filter out
                
                boolean present = false;

                final IChunkedOrderedIterator<ISPO> itr = store
                        .bulkFilterStatements(stmts, numStmts, present);
                
                assertSameSPOsAnyOrder(store,
                    
                    new SPO[]{
                        new SPO(x,a,v,
                                StatementEnum.Explicit),
                        new SPO(x,a,w,
                                StatementEnum.Explicit)
                    },
                    
                    itr,
                    true // ignore axioms
                    
                );
/*            
                while(itr.hasNext()) {
                    SPO spo = itr.next();
                    System.err.println(spo.toString(store));
                }
*/
            }
        
            { // filter in
                
                boolean present = true;

                final IChunkedOrderedIterator<ISPO> itr = store
                        .bulkFilterStatements(stmts, numStmts, present);
                
                assertSameSPOsAnyOrder(store,
                    
                    new SPO[]{
                        new SPO(x,a,y,
                                StatementEnum.Explicit),
                        new SPO(x,a,z,
                                StatementEnum.Explicit)
                    },
                    
                    itr,
                    true // ignore axioms
                    
                );
/*            
                while(itr.hasNext()) {
                    SPO spo = itr.next();
                    System.err.println(spo.toString(store));
                }
*/
            }
        
        } finally {
            
            store.__tearDownUnitTest();

        }

    }
    
    /**
     * Simple test of bulk SPO completion (value lookup).
     */
    public void testCompletion() {

        AbstractTripleStore store = getStore();
        
        try {

            final URI A = new URIImpl("http://www.bigdata.com/A");
//            final URI B = new URIImpl("http://www.bigdata.com/B");
//            final URI C = new URIImpl("http://www.bigdata.com/C");
//            final URI D = new URIImpl("http://www.bigdata.com/D");
//            final URI E = new URIImpl("http://www.bigdata.com/E");

            final URI V = new URIImpl("http://www.bigdata.com/V");
            final URI W = new URIImpl("http://www.bigdata.com/W");
            final URI X = new URIImpl("http://www.bigdata.com/X");
            final URI Y = new URIImpl("http://www.bigdata.com/Y");
            final URI Z = new URIImpl("http://www.bigdata.com/Z");
            
            final IV a = store.addTerm(A);
//            final IV b = store.addTerm(B);
//            final IV c = store.addTerm(C);
//            final IV d = store.addTerm(D);
//            final IV e = store.addTerm(E);
            
            final IV v = store.addTerm(V);
            final IV w = store.addTerm(W);
            final IV x = store.addTerm(X);
            final IV y = store.addTerm(Y);
            final IV z = store.addTerm(Z);
            
            SPO[] stmts = new SPO[] {
                new SPO(x,a,y,StatementEnum.Explicit),
                new SPO(x,a,z,StatementEnum.Inferred)
            };
            int numStmts = stmts.length;
                
            store.addStatements(stmts, numStmts);
            
//            store.commit();
    
            if (log.isInfoEnabled())
                log.info("\n" + store.dumpStore(true, true, false));

            stmts = new SPO[] {
                new SPO(x,a,y),
                new SPO(x,a,z),
                new SPO(x,a,v),
                new SPO(x,a,w)
            };
            numStmts = stmts.length;

            { // filter out and complete
                
                final IChunkedOrderedIterator<ISPO> itr = store
                        .bulkCompleteStatements(stmts, numStmts);
                
                assertSameSPOsAnyOrder(store,
                    
                    new SPO[]{
                        new SPO(x,a,y,
                                StatementEnum.Explicit),
                        new SPO(x,a,z,
                                StatementEnum.Inferred),
                        new SPO(x,a,v,
                                StatementEnum.Inferred),
                        new SPO(x,a,w,
                                StatementEnum.Inferred)
                    },
                    
                    itr,
                    true // ignore axioms
                    
                );
/*            
                while(itr.hasNext()) {
                    SPO spo = itr.next();
                    System.err.println(spo.toString(store));
                }
*/
            }
        
        } finally {
            
            store.__tearDownUnitTest();

        }

    }
    
}
