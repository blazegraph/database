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
 * Created on Oct 23, 2007
 */

package com.bigdata.rdf.rio;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.StatementHandler;
import org.openrdf.rio.StatementHandlerException;
import org.openrdf.sesame.constants.RDFFormat;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;

/**
 * Test loads an RDF/XML resource into a database and then verifies by re-parse
 * that all expected statements were made persistent in the database.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLoadAndVerify extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestLoadAndVerify() {
    }

    /**
     * @param name
     */
    public TestLoadAndVerify(String name) {
        super(name);
    }

    /** @todo use some RDF/XML that we can pack with the distribution. */
    public void test_loadAndVerify() throws Exception {
        
//      String file = "../rdf-data/nciOncology.owl";
        String file = "../rdf-data/alibaba_v41.rdf";
        
        if(!new File(file).exists()) {
            
            log.warn("Resource not found: "+file+", test skipped: "+getName());
            
            return;
            
        }

        AbstractTripleStore store = getStore();
        
        try {
    
        // avoid modification of the properties.
        Properties properties = new Properties(getProperties());
        
        // turn off RDFS closure for this test.
        properties.setProperty(DataLoader.Options.CLOSURE, ClosureEnum.None.toString());
        
        DataLoader dataLoader = new DataLoader(properties,store);
        
        // load into the datbase.
        dataLoader.loadData(file, "" /* baseURI */, RDFFormat.RDFXML);
        
        store.commit();
        
        if(store.isStable()) {
            
            store = reopenStore(store);
            
        }

        // re-parse and verify all statements exist in the db.
        final AtomicInteger nerrs = new AtomicInteger(0);
        {
            
            final AbstractTripleStore db = store;
            
            BasicRioLoader loader = new BasicRioLoader() {

                public StatementHandler newStatementHandler() {

                    return new StatementHandler() {

                        public void handleStatement(Resource s, URI p,
                                Value o) throws StatementHandlerException {
                            
                            if(!db.hasStatement(s, p, o)) {
                                
                                System.err.println("Could not find: ("+s+", "+p+", "+o+")");
                                
                                nerrs.incrementAndGet();
                                
                            }
                            
                        }

                    };

                }

            };
            
            loader.loadRdf(new BufferedReader(new InputStreamReader(
                    new FileInputStream(file))), ""/* baseURI */,
                    RDFFormat.RDFXML, false/* verify */);
            
        }
        
        assertEquals("nerrors",0,nerrs.get());

        } finally {
        
            store.closeAndDelete();
            
        }
        
    }
    
}
