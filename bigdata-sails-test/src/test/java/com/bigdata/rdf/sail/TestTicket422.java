/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Dec 6, 2011
 */

package com.bigdata.rdf.sail;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.TempTripleStore;

import info.aduna.iteration.CloseableIteration;

/**
 * Test suite for wrapping a {@link TempTripleStore} as a {@link BigdataSail}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestTicket422 extends ProxyBigdataSailTestCase {

    /**
     * 
     */
    public TestTicket422() {
    }

    /**
     * @param name
     */
    public TestTicket422(String name) {
        super(name);
    }

    public void test_wrapTempTripleStore() throws SailException, ExecutionException, InterruptedException {

        final BigdataSail sail = getSail();
        
        try {

            sail.initialize();
            
            final String namespace = sail.getNamespace();
            
            final BigdataSailConnection mainConn = sail.getUnisolatedConnection();
            
            try {
            
                final AbstractTripleStore mainTripleStore = mainConn.getTripleStore();
    
                if (mainTripleStore == null)
                    throw new UnsupportedOperationException();
                
                final TempTripleStore tempStore = new TempTripleStore(//
                        sail.getIndexManager().getTempStore(), //
                        mainConn.getProperties(), mainTripleStore);
    
            try {
    
                    // Note: The namespace of the tempSail MUST be distinct from the namespace of the main Sail.
                    final BigdataSail tempSail = new BigdataSail(namespace+"-"+UUID.randomUUID(), tempStore.getIndexManager(),
                            mainTripleStore.getIndexManager());
    
                try {
    
                    tempSail.initialize();
    
                        tempSail.create(new Properties());
                        
                    final BigdataSailConnection con = tempSail.getConnection();
    
                    try {
    
                        final CloseableIteration<? extends Statement, SailException> itr = con
                                .getStatements((Resource) null, (URI) null,
                                        (Value) null, (Resource) null);
    
                        try {
    
                            while (itr.hasNext()) {
    
                                itr.next();
    
                            }
                                
                        } finally {
    
                            itr.close();
    
                        }
    
                    } finally {
    
                        con.close();
    
                    }
    
                } finally {
    
                    tempSail.shutDown();
                    
                }
    
            } finally {
    
                tempStore.close();
                
            }
            
        } finally {
                
                mainConn.close();
                
            }
            
        } finally {

            sail.__tearDownUnitTest();
            
        }
        
    }
    
}
