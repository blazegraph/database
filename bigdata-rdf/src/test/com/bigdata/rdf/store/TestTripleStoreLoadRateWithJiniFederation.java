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
 * Created on May 21, 2007
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
import com.bigdata.service.DataService;
import com.bigdata.service.EmbeddedClient;

/**
 * Note: The commit flag is ignored for the {@link ScaleOutTripleStore}.
 * <p>
 * Note: The configuration options for the (meta)data services are set in their
 * respective <code>properties</code> files NOT by the System properties!
 * 
 * @see TestScaleOutTripleStoreWithJiniFederation, Notes on how to run this test
 *      suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTripleStoreLoadRateWithJiniFederation extends AbstractDistributedTripleStoreTestCase {

    /**
     * 
     */
    public TestTripleStoreLoadRateWithJiniFederation() {
    }

    /**
     * @param arg0
     */
    public TestTripleStoreLoadRateWithJiniFederation(String arg0) {
        super(arg0);
    }

    public Properties getProperties() {
        
        Properties properties = new Properties(super.getProperties());
        
        // Use the disk-backed store.
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());
        
        // Disable index partition moves (between data services).
        properties.setProperty(EmbeddedClient.Options.MAXIMUM_MOVES_PER_TARGET,"0");
        
        // setup overflow conditions.
        properties.setProperty(Options.INITIAL_EXTENT,""+Bytes.megabyte*500);
        properties.setProperty(Options.MAXIMUM_EXTENT,""+Bytes.megabyte*500);
//        properties.setProperty(Options.MAXIMUM_EXTENT,""+Bytes.megabyte*20);
//        properties.setProperty(Options.INITIAL_EXTENT,""+Bytes.megabyte*20);
        properties.setProperty(DataService.Options.OVERFLOW_ENABLED,"false");
        
        // name data directory for the unit test.
        properties.setProperty(EmbeddedClient.Options.DATA_DIR, getName());
        
        // turn off incremental truth maintenance.
        properties.setProperty(DataLoader.Options.CLOSURE,ClosureEnum.None.toString());

        // turn off text indexing.
        properties.setProperty(Options.TEXT_INDEX,"false");
        
        return properties;
        
    }
    
    public void test_loadNCIOncology() throws IOException {

        // load the data set.
//      store.getDataLoader().loadData("../rdf-data/nciOncology.owl", "", RDFFormat.RDFXML);
        store.getDataLoader().loadData("../rdf-data/Thesaurus.owl", "", RDFFormat.RDFXML);

        // compute the database at once closure.
//        store.getInferenceEngine().computeClosure(null/*focusStore*/);
        
    }

    public void test_U1() {
        
        new ConcurrentDataLoader(store, 20/*nthreads*/, 10000 /*bufferCapacity*/,  new File("../rdf-data/lehigh/U1"), new FilenameFilter(){

            public boolean accept(File dir, String name) {
                if(name.endsWith(".owl")) return true;
                return false;
            }
            
        });
        
    }
    
    public void test_U10() {
        
        new ConcurrentDataLoader(store, 20/*nthreads*/, 100000 /*bufferCapacity*/,  new File("../rdf-data/lehigh/U10"), new FilenameFilter(){

            public boolean accept(File dir, String name) {
                if(name.endsWith(".owl")) return true;
                return false;
            }
            
        });
        
    }

    
    public void test_U50() {
        
        new ConcurrentDataLoader(store, 10/*nthreads*/, 20000 /*bufferCapacity*/,  new File("../rdf-data/lehigh/U50"), new FilenameFilter(){

            public boolean accept(File dir, String name) {
                if(name.endsWith(".owl")) return true;
                return false;
            }
            
        });
        
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
     * You must also specify
     * 
     * <pre>
     *   -Djava.security.policy=policy.all
     * </pre>
     * 
     * and probably want to specify
     * 
     * <pre>
     *  -Dcom.sun.jini.jeri.tcp.useNIO=true
     * </pre>
     * 
     * as well.
     * 
     * @todo support load of the ontology as well?
     * 
     * @todo make this part of the {@link ConcurrentDataLoader} and modify so
     *       that you can run against any pre-existing database, including a
     *       jini-based bigdata federation, or create a new one from the defined
     *       properties.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        final int nthreads = Integer.parseInt(System.getProperty("nthreads","20")); 
        
        final int bufferCapacity = Integer.parseInt(System.getProperty("bufferCapacity","100000")); 
        
        final String file = System.getProperty("documents.directory");
  
        if(file==null) throw new RuntimeException("Required property 'documents.directory' was not specified");
        
        TestTripleStoreLoadRateWithJiniFederation test = new TestTripleStoreLoadRateWithJiniFederation("test");
        
        test.setUp();
        
        new ConcurrentDataLoader(test.store, nthreads, bufferCapacity,
                new File(file), new FilenameFilter() {

            public boolean accept(File dir, String name) {
//                if(name.endsWith(".owl")) return true;
                return true;
//                return false;
            }
            
        });
        
        test.tearDown();
        
        System.out.println("Exiting normally.");
        
        System.exit(0);
        
    }
    
}
