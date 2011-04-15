/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Apr 13, 2011
 */

package com.bigdata.rdf.sail.webapp;

import java.io.File;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 * Stub runs the REST API based on the <code>web.xml</code> file in the same
 * package.
 * <p>
 * Note: The jetty webapp and xml dependencies are required in addition to those
 * dependencies required for the embedded {@link NanoSparqlServer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class WebAppUnassembled
{

    /**
     * 
     * @param args
     *            <code>option(s)</code> where options includes any of:
     *            <dl>
     *            <dt>-port <i>port</i></dt>
     *            <dd>The port on which to start the service.</dd>
     *            <dt>-config <i>file</i></dt>
     *            <dd>The path of the <code>web.xml</code> file to use.</dd>
     *            </dl>
     *            
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {

        // default port
        int port = 80;
        
        // default config file.
        String file = "bigdata-war/src/resources/WEB-INF/web.xml";

        /*
         * Handle all arguments starting with "-". These should appear before
         * any non-option arguments to the program.
         */
        int i = 0;
        while (i < args.length) {
            final String arg = args[i];
            if (arg.startsWith("-")) {
                if (arg.equals("-port")) {
                    final String s = args[++i];
                    if (port < 0) {
                        throw new RuntimeException(
                                "port must be non-negative, not: " + s);
                    }
                    port = Integer.valueOf(s);
                } else if (arg.equals("-config")) {
                    final String s = args[++i];
                    if (!new File(s).exists()) {
                        throw new RuntimeException("Not found: " + s);
                    }
                    file = s;
                } else {
                    throw new RuntimeException("Unknown argument: " + arg);
                }
            } else {
                throw new RuntimeException("Unknown argument: " + arg);
            }
            i++;
        }
        
        final WebAppContext webapp = new WebAppContext();
        webapp.setDescriptor(file);
        webapp.setResourceBase(".");// TODO flot resources.
        webapp.setContextPath("/");
        webapp.setParentLoaderPriority(true);

        System.out.println("Port: " + port);

        final Server server = new Server(port);

        server.setHandler(webapp);

        server.start();
        
        System.out.println("Running");
        
        server.join();

        System.out.println("Bye");

    }

}
