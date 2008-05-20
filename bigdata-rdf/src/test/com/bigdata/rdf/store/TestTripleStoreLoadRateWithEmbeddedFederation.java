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
//        properties.setProperty(EmbeddedClient.Options.MAXIMUM_MOVES_PER_TARGET,"0");
        
        /*
         * setup overflow conditions - can be easily modified to trigger
         * overflow early or late or to disable overflow all together.
         * 
         * Note: Running with 5M initial and maximum extent is something of a
         * stress test.  Under Windows it will eventually trigger a JVM bug
         * dealing with large channel to channel transfers during an index
         * segment build, but it should otherwise run correctly.
         */
//      properties.setProperty(DataService.Options.OVERFLOW_ENABLED,"false");
//        properties.setProperty(Options.MAXIMUM_EXTENT,""+Bytes.megabyte*5);
//        properties.setProperty(Options.INITIAL_EXTENT,""+Bytes.megabyte*5);
//      properties.setProperty(Options.MAXIMUM_EXTENT,""+Bytes.megabyte*100);
//      properties.setProperty(Options.INITIAL_EXTENT,""+Bytes.megabyte*100);
        properties.setProperty(Options.MAXIMUM_EXTENT,""+Bytes.megabyte*200);
        properties.setProperty(Options.INITIAL_EXTENT,""+Bytes.megabyte*200);
//      properties.setProperty(Options.INITIAL_EXTENT,""+Bytes.megabyte*500);
//        properties.setProperty(Options.MAXIMUM_EXTENT,""+Bytes.megabyte*500);
        
        // Do not hold onto old resources.
        properties.setProperty(StoreManager.Options.MIN_RELEASE_AGE, "0");
        
        // control the #of data services.
        properties.setProperty(EmbeddedClient.Options.NDATA_SERVICES, "1");

        // turn off incremental truth maintenance.
        properties.setProperty(DataLoader.Options.CLOSURE,ClosureEnum.None.toString());

        // turn off text indexing.
//        properties.setProperty(Options.TEXT_INDEX,"false");

        // turn off statement identifiers.
//        properties.setProperty(Options.STATEMENT_IDENTIFIERS,"false");

        // Enable the o/s specific statistics collection for the test run.
//        properties.setProperty(EmbeddedClient.Options.COLLECT_PLATFORM_STATISTICS,"true");

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


    final int nthreads = 10;
    
    final int bufferCapacity = 100000;
    
    final boolean validate = false;
    
    final FilenameFilter filter = new FilenameFilter() {

        public boolean accept(File dir, String name) {
            if (name.endsWith(".owl"))
                return true;
            return false;
        }

    };

    public void test_U1() throws InterruptedException {
        
        final File file = new File("../rdf-data/lehigh/U1");
//      final File file = new File("../rdf-data/lehigh/U1/University0_0.owl");
      
        RDFLoadAndValidateHelper helper = new RDFLoadAndValidateHelper(client,
                nthreads, bufferCapacity, file, filter);

        helper.load(store);

        if(validate)
        helper.validate(store);
        
        helper.shutdownNow();
        
    }
    
    public void test_U10() throws InterruptedException {

        final File file = new File("../rdf-data/lehigh/U10");

        RDFLoadAndValidateHelper helper = new RDFLoadAndValidateHelper(client,
                nthreads, bufferCapacity, file, filter);

        helper.load(store);
        
        if(validate)
        helper.validate(store);
        
        helper.shutdownNow();
        
    }

    public void test_U50() throws InterruptedException {

        final File file = new File("../rdf-data/lehigh/U50");

        RDFLoadAndValidateHelper helper = new RDFLoadAndValidateHelper(client,
                nthreads, bufferCapacity, file, filter);

        helper.load(store);

        if(validate)
        helper.validate(store);
        
        helper.shutdownNow();
        
    }
    
    /**
     * 
     * <dl>
     * <dt>-Dnthreads</dt>
     * <dd>#of threads to use.</dd>
     * <dt>-DbufferCapacity</dt>
     * <dd>Capacity of the statement buffers.</dd>
     * <dt>-Ddocuments.directory</dr>
     * <dd>The file or directory to be loaded (recursive processing).</dd>
     * </dl>
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        final int nthreads = Integer.parseInt(System.getProperty("nthreads","20")); 
        
        final int bufferCapacity = Integer.parseInt(System.getProperty("bufferCapacity","100000")); 
        
        final boolean validate = Boolean.parseBoolean(System.getProperty("validate",
                "false"));

        final String fileStr = System.getProperty("documents.directory");

        if (fileStr == null)
            throw new RuntimeException(
                    "Required property 'documents.directory' was not specified");

        final File file = new File(fileStr);

        TestTripleStoreLoadRateWithEmbeddedFederation test = new TestTripleStoreLoadRateWithEmbeddedFederation("test");
        
        test.setUp();

        RDFLoadAndValidateHelper helper = new RDFLoadAndValidateHelper(
                test.client, nthreads, bufferCapacity, file, test.filter);

        helper.load(test.store);

        if(validate)
            helper.validate(test.store);

        helper.shutdownNow();
        
        test.tearDown();
        
        System.out.println("Exiting normally.");
        
        System.exit(0);
        
    }
        
}
