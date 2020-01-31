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
package com.bigdata.rdf.sail.webapp;

import java.util.LinkedHashMap;
import java.util.Map;

import org.eclipse.jetty.server.Server;

import com.bigdata.Banner;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.util.httpd.Config;

/**
 * Utility class provides a simple SPARQL end point with a REST API.  
 * It is intended to be used for simple deployment via an executable jar file.
 * 
 * @author beebs
 * 
 * @see <a
 *      href="https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=NanoSparqlServer">
 *      NanoSparqlServer </a> on the wiki.
 */
public class StandaloneNanoSparqlServer extends NanoSparqlServer {

	public static void main(String[] args) throws Exception {
		Banner.banner();

        int port = -1;
        String namespace = "kb";
        int queryThreadPoolSize = ConfigParams.DEFAULT_QUERY_THREAD_POOL_SIZE;
        boolean forceOverflow = false;
        Long readLock = null;
     
        /*
         * Note: This default will locate the jetty.xml resource that is bundled
         * with the JAR. This preserves the historical behavior. If you want to
         * use a different jetty.xml file, just override this property on the
         * command line.
         */
        String jettyXml = System.getProperty(//
                SystemProperties.JETTY_XML,//
                "jetty.xml"//
//                SystemProperties.DEFAULT_JETTY_XML
                );
        
        String propertyFile = System.getProperty(
        		SystemProperties.BIGDATA_PROPERTY_FILE ,
        		"RWStore.properties"
        		);
        
        String portStr = System.getProperty(
        		SystemProperties.JETTY_PORT ,
        		Integer.toString(Config.BLAZEGRAPH_HTTP_PORT)
        		);
        
        port = Integer.parseInt(portStr);

        /*
         * Handle all arguments starting with "-". These should appear before
         * any non-option arguments to the program.
         */
        int i = 0;
        while (i < args.length) {
            final String arg = args[i];
            if (arg.startsWith("-")) {
                if (arg.equals("-forceOverflow")) {
                    forceOverflow = true;
                } else if (arg.equals("-nthreads")) {
                    final String s = args[++i];
                    queryThreadPoolSize = Integer.valueOf(s);
                    if (queryThreadPoolSize < 0) {
                        usage(1/* status */,
                                "-nthreads must be non-negative, not: " + s);
                    }
                } else if (arg.equals("-readLock")) {
                    final String s = args[++i];
                    readLock = Long.valueOf(s);
                    if (readLock != ITx.READ_COMMITTED
                            && !TimestampUtility.isCommitTime(readLock
                                    .longValue())) {
                        usage(1/* status */,
                                "Read lock must be commit time or -1 (MINUS ONE) to assert a read lock on the last commit time: "
                                        + readLock);
                    }
                } else if (arg.equals("-jettyXml")) {
                    jettyXml = args[++i];
                } else {
                    usage(1/* status */, "Unknown argument: " + arg);
                }
            } else {
                break;
            }
            i++;
        }

        final Map<String, String> initParams = new LinkedHashMap<String, String>();

        initParams.put(
                ConfigParams.PROPERTY_FILE,
                propertyFile);

        initParams.put(ConfigParams.NAMESPACE,
                namespace);

        initParams.put(ConfigParams.QUERY_THREAD_POOL_SIZE,
                Integer.toString(queryThreadPoolSize));

        initParams.put(
                ConfigParams.FORCE_OVERFLOW,
                Boolean.toString(forceOverflow));

        if (readLock != null) {
            initParams.put(
                    ConfigParams.READ_LOCK,
                    Long.toString(readLock));
        }
        
        //Set the resource base to inside of the jar file
		System.setProperty("jetty.home",
				StandaloneNanoSparqlServer.class.getResource("/war").toExternalForm());

        // Create the service.
        final Server server = StandaloneNanoSparqlServer.newInstance(port, jettyXml,
                null/* indexManager */, initParams);


        awaitServerStart(server);

        System.out.println("\n\nWelcome to the Blazegraph(tm) Database.\n");
        //BLZG-1377 Included for legacy support.
        //BLZG-1812:  Updated for correction of port override.
        System.out.println("Go to http://" + getHost() + ":" + port + "/" + Config.BIGDATA_PATH + "/ to get started.");

        
        // Wait for the service to terminate.
        server.join();
	}

}
