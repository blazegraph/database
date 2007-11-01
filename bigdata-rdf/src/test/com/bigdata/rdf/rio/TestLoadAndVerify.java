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
 * Created on Oct 23, 2007
 */

package com.bigdata.rdf.rio;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
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
        
        AbstractTripleStore store = getStore();
        
//        String file = "data/nciOncology.owl";
        String file = "data/alibaba_v41.rdf";
    
        DataLoader dataLoader = store.getDataLoader();
        
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
        
        store.closeAndDelete();
        
        assertEquals("nerrors",0,nerrs.get());
        
    }
    
}
