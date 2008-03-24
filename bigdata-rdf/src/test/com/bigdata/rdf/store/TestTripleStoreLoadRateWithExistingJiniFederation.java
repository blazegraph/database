/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jan 14, 2008
 */

package com.bigdata.rdf.store;

import java.io.File;
import java.io.FilenameFilter;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;

import com.bigdata.service.BigdataClient;

/**
 * Class designed to connect to an existing bigdata federation using jini and
 * bulk load a data set into that federation.
 * <p>
 * Note: Jini MUST be running.
 * <p>
 * The metadata service and the data services MUST be running
 * <p>
 * Note: The configuration options for the (meta)data services are set in their
 * respective <code>properties</code> files NOT by the System properties!
 * 
 * @todo support distributed client using hash(filename) MOD N to select host
 * 
 * @todo provide dropIndex so that we can guarentee a cleared federation or at
 *       least warn if the data services already have data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTripleStoreLoadRateWithExistingJiniFederation {

    protected static Logger log = Logger.getLogger(TestTripleStoreLoadRateWithExistingJiniFederation.class);
    
    /**
     * 
     * <dl>
     * 
     * <dt>-DminDataServices</dt>
     * <dd>The minium #of data services that must be available before the
     * client will start (1 or more). In addition, there must be a metadata
     * service available for the client to run.</dd>
     * 
     * <dt>-Dtimeout</dt>
     * <dd>Timeout (ms) that the client will wait to discover the metadata
     * service and required #of data services (default 20000).</dd>
     * 
     * <dt> -Dnclients </dt>
     * <dd> The #of client processes that will share the data load process. Each
     * client process MUST be started independently in its own JVM. All clients
     * MUST have access to the files to be loaded. </dd>
     * 
     * <dt> -DclientNum </dt>
     * <dd> The client host identifier in [0:nclients-1]. The clients will load
     * files where <code>filename.hashCode() % nclients == clientNum</code>.
     * If there are N clients loading files using the same pathname to the data
     * then this will divide the files more or less equally among the clients.
     * (If the data to be loaded are pre-partitioned then you do not need to
     * specify either <i>nclients</i> or <i>clientNum</i>.) </dd>
     * 
     * <dt>-Dnthreads</dt>
     * <dd>#of threads to use <em>per client</em>.</dd>
     * 
     * <dt>-DbufferCapacity</dt>
     * <dd>Capacity of the statement buffers.</dd>
     * 
     * <dt>-Ddocuments.directory</dr>
     * <dd>The file or directory to be loaded (recursive processing).</dd>
     * </dl>
     * 
     * You must also specify
     * 
     * <pre>
     *            -Djava.security.policy=policy.all
     * </pre>
     * 
     * and probably want to specify
     * 
     * <pre>
     *           -Dcom.sun.jini.jeri.tcp.useNIO=true
     * </pre>
     * 
     * as well.
     * 
     * @todo support load of the ontology as well?
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        
        final int minDataServices = Integer.parseInt(System.getProperty("minDataServices","2")); 

        final long timeout = Long.parseLong(System.getProperty("timeout","20000")); 

        final int nthreads = Integer.parseInt(System.getProperty("nthreads","20")); 
        
        final int bufferCapacity = Integer.parseInt(System.getProperty("bufferCapacity","100000")); 
        
        final int nclients = Integer.parseInt(System.getProperty("nclients","1"));

        final int clientNum = Integer.parseInt(System.getProperty("clientNum","0"));
        
        final String file = System.getProperty("documents.directory");
  
        if(file==null) throw new RuntimeException("Required property 'documents.directory' was not specified");
        
        /**
         * Starts in {@link #setUp()}.
         */
        BigdataClient client = BigdataClient.newInstance(
                new String[] { "src/resources/config/standalone/Client.config"
//                        , BigdataClient.CLIENT_LABEL+groups
                        });

        /*
         * Await at least N data services and one metadata service (otherwise
         * abort).
         */
        final int N = client.awaitServices(minDataServices, timeout);
        
        System.err.println("Will run with "+N+" data services");
        
        ScaleOutTripleStore store = new ScaleOutTripleStore(client.connect(),System.getProperties());

        if(store.getSPOIndex()==null) {
    
            log.info("Registering scale-out indices");
            
            store.registerIndices();
            
        }
        
        new ConcurrentDataLoader(store, nthreads, bufferCapacity,
                new File(file), new FilenameFilter() {

            public boolean accept(File dir, String name) {
//                if(name.endsWith(".owl")) return true;
                return true;
//                return false;
            }
            
        }, ""/* baseURL */, RDFFormat.RDFXML/* fallback */,
                false/* autoFlush */, nclients, clientNum);
        
        client.shutdownNow();
        
        System.out.println("Exiting normally.");
        
        System.exit(0);
        
    }

}
