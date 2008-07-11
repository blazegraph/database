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

import com.bigdata.journal.ITx;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;

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
     * <p>
     * 
     * Finally, you must specify the path of the Jini configuration file for
     * the client on the command line (as an argument to the program).
     * 
     * @todo support load of the ontology as well?
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        
        final int minDataServices = Integer.parseInt(System.getProperty("minDataServices","2")); 

        final long timeout = Long.parseLong(System.getProperty("timeout","20000")); 

        final String namespace = System.getProperty("namespace","test");

        final int nthreads = Integer.parseInt(System.getProperty("nthreads","20")); 
        
        final int bufferCapacity = Integer.parseInt(System.getProperty("bufferCapacity","100000")); 
        
        final boolean validate = Boolean.parseBoolean(System.getProperty("validate",
        "false"));

        final int nclients = Integer.parseInt(System.getProperty("nclients","1"));

        final int clientNum = Integer.parseInt(System.getProperty("clientNum","0"));
        
        final String fileStr = System.getProperty("documents.directory");

        if (fileStr == null)
            throw new RuntimeException(
                    "Required property 'documents.directory' was not specified");

        final File file = new File(fileStr);

        if (args.length == 0) {

            System.err.println("usage: config");

            System.exit(1);

        }

        final String jiniConfig = args[0]; 

        System.out.println("Using: "+jiniConfig);
        
        /**
         * Starts in {@link #setUp()}.
         */
        JiniClient client = JiniClient.newInstance(
                new String[] { jiniConfig
//                        , BigdataClient.CLIENT_LABEL+groups
                        });

        JiniFederation fed = client.connect();
        
        /*
         * Await at least N data services and one metadata service (otherwise
         * abort).
         */
        final int N = fed.awaitServices(minDataServices, timeout);
        
        System.err.println("Will run with "+N+" data services");

        ScaleOutTripleStore store = new ScaleOutTripleStore(client
                .getFederation(), namespace, ITx.UNISOLATED, client
                .getProperties());

        if(!new SPORelation(store.getIndexManager(), store.getNamespace()
                + store.NAME_SPO_RELATION, store.getTimestamp(), store.getProperties()).exists()) {
            
            // Presume that the KB does not exist.
            
            store.create();
            
        }
        
        final FilenameFilter filter = new FilenameFilter() {

            public boolean accept(File dir, String name) {
                if (name.endsWith(".owl"))
                    return true;
                return false;
            }

        };
        
        RDFLoadAndValidateHelper helper = new RDFLoadAndValidateHelper(
                client, nthreads, bufferCapacity, file, filter, nclients, clientNum);

        helper.load(store);

        if(validate)
        helper.validate(store);

        helper.shutdownNow();
        
        client.disconnect(true/*immediateShutdown*/);
        
        System.out.println("Exiting normally.");
        
        System.exit(0);
        
    }

}
