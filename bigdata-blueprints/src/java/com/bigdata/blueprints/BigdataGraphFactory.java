/**
Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
package com.bigdata.blueprints;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.remote.BigdataSailFactory;


/**
 * Helper class to create BigdataGraph instances.
 * 
 * @author mikepersonick
 *
 */
public class BigdataGraphFactory  {

    protected static final transient Logger log = Logger.getLogger(BigdataGraphFactory.class);
    
    /**
     * Connect to a remote bigdata instance.
     */
    public static BigdataGraph connect(final String host, final int port) {
        return connect("http://"+host+":"+port);
    }
    
    /**
     * Connect to a remote bigdata instance.
     */
    public static BigdataGraph connect(final String serviceEndpoint) {

    	//Ticket #1182:  centralize rewriting in the SAIL factory.

        return new BigdataGraphClient(serviceEndpoint);
    }

    /**
     * Open an existing persistent local bigdata instance.  If a journal does
     * not exist at the specified location and the boolean create flag is true
     * a journal will be created at that location.
     */
    public static BigdataGraph open(final String file, final boolean create) throws Exception {
        final BigdataSail sail = BigdataSailFactory.openSail(file, create);
        sail.initialize();
        return new BigdataGraphEmbedded(sail);
    }

    /**
	 * Create a persistent local bigdata instance. If a journal does not exist
	 * at the specified location, then a journal will be created at that
	 * location.
	 */
    public static BigdataGraph create(final String file) throws Exception {
        final BigdataSail sail = BigdataSailFactory.openSail(file, true);
        sail.initialize();
        return new BigdataGraphEmbedded(sail);
    }

    /**
     * Create a new local in-memory bigdata instance.
     */
    public static BigdataGraph create() throws Exception {
        return create(BigdataRDFFactory.INSTANCE);
    }
    
    /**
     * Create a new local in-memory bigdata instance with the supplied value
     * factory.
     */
    public static BigdataGraph create(final BlueprintsValueFactory vf) 
            throws Exception {
        return create(vf, new Properties());
    }
    
    /**
     * Create a new local in-memory bigdata instance with the supplied value
     * factory.
     */
    public static BigdataGraph create(final BlueprintsValueFactory vf, 
            final Properties props) throws Exception {
        final BigdataSail sail = BigdataSailFactory.createSail();
        sail.initialize();
        return new BigdataGraphEmbedded(sail, vf, props);
    }
    
//    /**
//     * Create a new persistent local bigdata instance.
//     */
//    public static BigdataGraph create(final String file) 
//            throws Exception {
//        final BigdataSail sail = BigdataSailFactory.createSail(file);
//        sail.initialize();
//        return new BigdataGraphEmbedded(sail);
//    }
    
}
