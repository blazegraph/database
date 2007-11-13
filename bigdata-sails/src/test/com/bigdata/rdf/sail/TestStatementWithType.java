/*
 * Copyright SYSTAP, LLC 2006-2007.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Nov 12, 2007
 */

package com.bigdata.rdf.sail;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sesame.sail.SailInitializationException;
import org.openrdf.sesame.sail.SailUpdateException;

import com.bigdata.rdf.inf.SPOAssertionBuffer;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.StatementWithType;

/**
 * Test for access to the {@link StatementEnum}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestStatementWithType extends AbstractBigdataRdfRepositoryTestCase {

    /**
     * 
     */
    public TestStatementWithType() {
    }

    /**
     * @param arg0
     */
    public TestStatementWithType(String arg0) {
        super(arg0);
    }

    /**
     * Test using the nation API of adding explicit, inferred, and axiom
     * {@link SPO}s.
     * 
     * @throws SailInitializationException 
     */
    public void test_addInferredExplicitAxiom() throws SailInitializationException {

        final long NULL = IRawTripleStore.NULL;
        
        AbstractTripleStore store = repo.getDatabase();

        try {

            URI S1 = new URIImpl("http://www.bigdata.com/s1");
            URI S2 = new URIImpl("http://www.bigdata.com/s2");
            URI S3 = new URIImpl("http://www.bigdata.com/s3");
            URI P = new URIImpl("http://www.bigdata.com/p");
            URI O = new URIImpl("http://www.bigdata.com/o");
            
            long s1 = store.addTerm(S1);
            long s2 = store.addTerm(S2);
            long s3 = store.addTerm(S3);
            long p = store.addTerm(P);
            long o = store.addTerm(O);

            SPOAssertionBuffer buffer = new SPOAssertionBuffer(store,
                    null/* filter */, 100/* capacity */, false/* justified */);

            buffer.add(new SPO(s1, p, o, StatementEnum.Explicit));
            buffer.add(new SPO(s2, p, o, StatementEnum.Inferred));
            buffer.add(new SPO(s3, p, o, StatementEnum.Axiom));

            buffer.flush();

            store.commit();

            AbstractTestCase.assertSameSPOs(new SPO[] {
                    new SPO(s1, p, o, StatementEnum.Explicit),
                    new SPO(s2, p, o, StatementEnum.Inferred),
                    new SPO(s3, p, o, StatementEnum.Axiom),
                    },
                    store.getAccessPath(NULL, NULL, NULL).iterator()
                    );

            {

                StatementWithType stmt = (StatementWithType)repo.getStatements(S1,P,O).next();
                
                assertEquals(StatementEnum.Explicit,stmt.getStatementType());
                
            }
            
            {

                StatementWithType stmt = (StatementWithType)repo.getStatements(S2,P,O).next();
                
                assertEquals(StatementEnum.Inferred,stmt.getStatementType());
                
            }
            
            {

                StatementWithType stmt = (StatementWithType)repo.getStatements(S3,P,O).next();
                
                assertEquals(StatementEnum.Axiom,stmt.getStatementType());
                
            }
            
            store.dumpStore();

        } finally {

            store.closeAndDelete();
            
        }
            
    }
    
    /**
     * Test using the Sesame API to statements and verify {@link StatementWithType}.
     * 
     * @throws SailInitializationException 
     * @throws SailUpdateException 
     */
    public void test_addStatements() throws SailInitializationException, SailUpdateException {

        AbstractTripleStore store = repo.getDatabase();

        try {

            URI S1 = new URIImpl("http://www.bigdata.com/s1");
            URI S2 = new URIImpl("http://www.bigdata.com/s2");
            URI S3 = new URIImpl("http://www.bigdata.com/s3");
            URI P = new URIImpl("http://www.bigdata.com/p");
            URI O = new URIImpl("http://www.bigdata.com/o");

            repo.startTransaction();
            repo.addStatement(S1, P, O);
            repo.addStatement(S2, P, O);
            repo.addStatement(S3, P, O);
            repo.commitTransaction();

            {

                StatementWithType stmt = (StatementWithType)repo.getStatements(S1,P,O).next();
                
                assertEquals(StatementEnum.Explicit,stmt.getStatementType());
                
            }
            
            {

                StatementWithType stmt = (StatementWithType)repo.getStatements(S2,P,O).next();
                
                assertEquals(StatementEnum.Explicit,stmt.getStatementType());
                
            }
            
            {

                StatementWithType stmt = (StatementWithType)repo.getStatements(S3,P,O).next();
                
                assertEquals(StatementEnum.Explicit,stmt.getStatementType());
                
            }
            
            {

                StatementWithType stmt = (StatementWithType) repo
                        .getStatements(S3, URIImpl.RDF_TYPE,
                                URIImpl.RDFS_RESOURCE)
                                .next();
                
                assertEquals(StatementEnum.Inferred,stmt.getStatementType());
                
            }
            
            store.dumpStore();

        } finally {

            store.closeAndDelete();
            
        }
            
    }
    
}
