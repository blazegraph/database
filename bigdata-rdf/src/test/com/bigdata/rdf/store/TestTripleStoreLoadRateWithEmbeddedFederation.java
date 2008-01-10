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
 * Created on Jul 25, 2007
 */

package com.bigdata.rdf.store;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Properties;

import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;

/**
 * Variant of {@link TestTripleStoreLoadRateWithJiniFederation} that tests with an
 * embedded bigdata federation and therefore does not incur costs for network
 * IO.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTripleStoreLoadRateWithEmbeddedFederation extends
        AbstractEmbeddedTripleStoreTestCase {

    /**
     * 
     */
    public TestTripleStoreLoadRateWithEmbeddedFederation() {
        super();
    }

    /**
     * @param arg0
     */
    public TestTripleStoreLoadRateWithEmbeddedFederation(String arg0) {
        super(arg0);
    }

    protected Properties getProperties() {
        
        Properties properties = new Properties(super.getProperties());
        
        // turn off incremental truth maintenance.
        properties.setProperty(DataLoader.Options.CLOSURE,ClosureEnum.None.toString());
        
        return properties;
        
    }
    
    public void test_loadNCIOncology() throws IOException {

        // load the data set.
//        store.getDataLoader().loadData("../rdf-data/nciOncology.owl", "", RDFFormat.RDFXML);
        store.getDataLoader().loadData("../rdf-data/Thesaurus.owl", "", RDFFormat.RDFXML);

        // compute the database at once closure.
//        store.getInferenceEngine().computeClosure(null/*focusStore*/);
        
    }

    public void test_U1() {
        
        new ConcurrentDataLoader(store, 3/*nthreads*/, 10000 /*bufferCapacity*/, new File("../rdf-data/lehigh/U1"), new FilenameFilter(){

            public boolean accept(File dir, String name) {
                if(name.endsWith(".owl")) return true;
                return false;
            }
            
        });
        
    }
    
    /**
     * @todo setup an experiment driver to explore the parameter space for
     *       nthreads, buffer size, LUBM size, federation parameters, etc?
     */
    public void test_U10() {
        
        new ConcurrentDataLoader(store, 20/*nthreads*/, 20000 /*bufferCapacity*/, new File("../rdf-data/lehigh/U10"), new FilenameFilter(){

            public boolean accept(File dir, String name) {
                if(name.endsWith(".owl")) return true;
                return false;
            }
            
        });
        
    }

}
