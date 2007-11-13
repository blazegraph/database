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
 * Created on Nov 13, 2007
 */

package com.bigdata.rdf.sail;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

/**
 * Test suite for SAIL transaction semantics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSAILTransactionSemantics extends AbstractBigdataRdfRepositoryTestCase {

    /**
     * 
     */
    public TestSAILTransactionSemantics() {
    }

    /**
     * @param arg0
     */
    public TestSAILTransactionSemantics(String arg0) {
        super(arg0);
    }

    /**
     * Test the commit semantics in the context of a read-committed view of the
     * database.
     */
    public void test_commit() {

        // 2nd SAIL wrapping a read-committed view of the same database.
        BigdataRdfRepository view = repo.asReadCommittedView();

        repo.startTransaction();
        
        URI s = new URIImpl("http://www.bigdata.com/s");
        URI p = new URIImpl("http://www.bigdata.com/p");
        URI o = new URIImpl("http://www.bigdata.com/o");
        
        try {
            
            // add the statement.
            repo.addStatement(s, p, o);
            
            // visible in the repo.
            assertTrue(repo.hasStatement(s,p,o));

            // not visible in the view.
            assertFalse(view.hasStatement(s,p,o));
            
            // commit the transaction.
            repo.commitTransaction();
            
        } catch(Throwable t) {
            
            log.error(t);
            
            // discard the write set.
            repo.abortTransaction();
            
            fail("Unexpected exception: "+t, t);
            
        }
        
        // now visible in the view.
        assertTrue(view.hasStatement(s,p,o));
        
    }
    
    /**
     * Test of abort semantics.
     */
    public void test_abort() {
        
        class AbortException extends RuntimeException {
            private static final long serialVersionUID = 1L;
        }

        repo.startTransaction();
        
        URI s = new URIImpl("http://www.bigdata.com/s");
        URI p = new URIImpl("http://www.bigdata.com/p");
        URI o = new URIImpl("http://www.bigdata.com/o");
        
        try {
            
            // add the statement.
            repo.addStatement(s, p, o);
            
            // visible in the repo.
            assertTrue(repo.hasStatement(s,p,o));

            throw new AbortException();
            
        } catch(AbortException ex) {
            
            // discard the write set.
            repo.abortTransaction();

            // no longer visible in the repo.
            assertFalse(repo.hasStatement(s,p,o));

        } catch(Throwable t) {
            
            // discard the write set.
            repo.abortTransaction();
            
            fail("Unexpected exception: "+t, t);
            
        }
        
    }
    
}
