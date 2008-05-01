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

import com.bigdata.journal.BufferMode;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;
import com.bigdata.resources.StoreManager;
import com.bigdata.service.DataService;
import com.bigdata.service.EmbeddedClient;

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
        
        // Use the disk-backed store.
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());
        
        // name data directory for the unit test.
        properties.setProperty(EmbeddedClient.Options.DATA_DIR, getName());

        // Disable index partition moves (between data services).
        properties.setProperty(EmbeddedClient.Options.MAXIMUM_MOVES_PER_TARGET,"0");
        
        // Disable the o/s specific statistics collection for the test run.
        properties.setProperty(EmbeddedClient.Options.COLLECT_PLATFORM_STATISTICS,"false");
        
        /*
         * setup overflow conditions - can be easily modified to trigger
         * overflow early or late or to disable overflow all together.
         */
//      properties.setProperty(DataService.Options.OVERFLOW_ENABLED,"false");
//      properties.setProperty(Options.INITIAL_EXTENT,""+Bytes.megabyte*500);
//        properties.setProperty(Options.MAXIMUM_EXTENT,""+Bytes.megabyte*500);
        properties.setProperty(Options.MAXIMUM_EXTENT,""+Bytes.megabyte*5);
        properties.setProperty(Options.INITIAL_EXTENT,""+Bytes.megabyte*5);
        
        // Do not hold onto old resources.
        properties.setProperty(StoreManager.Options.MIN_RELEASE_AGE, "0");
        
        // control the #of data services.
        properties.setProperty(EmbeddedClient.Options.NDATA_SERVICES, "1");

        // turn off incremental truth maintenance.
        properties.setProperty(DataLoader.Options.CLOSURE,ClosureEnum.None.toString());

        // turn off text indexing.
//        properties.setProperty(Options.TEXT_INDEX,"false");

        // change the default port for httpd exposed by the load balancer. 
        properties.setProperty(com.bigdata.service.LoadBalancerService.Options.HTTPD_PORT,"8080");

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
        
        new ConcurrentDataLoader(store, 20/*nthreads*/, 100000 /*bufferCapacity*/, new File("../rdf-data/lehigh/U10"), new FilenameFilter(){

            public boolean accept(File dir, String name) {
                if(name.endsWith(".owl")) return true;
                return false;
            }
            
        });
        
    }

    public void test_U20() {
        
        new ConcurrentDataLoader(store, 20/*nthreads*/, 100000 /*bufferCapacity*/, new File("../rdf-data/lehigh/U20"), new FilenameFilter(){

            public boolean accept(File dir, String name) {
                if(name.endsWith(".owl")) return true;
                return false;
            }
            
        });
        
    }
    
}
