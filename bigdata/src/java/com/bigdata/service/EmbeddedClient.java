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

package com.bigdata.service;

import java.util.Properties;

/**
 * A client for an embedded federation (the client and the data services all run
 * in the same process).
 * 
 * @see EmbeddedFederation
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmbeddedClient extends AbstractClient {

    /**
     * 
     * @param properties
     *            See {@link EmbeddedFederation.Options}.
     */
    public EmbeddedClient(Properties properties) {

        super(properties);
        
    }
    
    /**
     * The federation and <code>null</code> iff not connected.
     */
    private EmbeddedFederation fed = null;

    synchronized public boolean isConnected() {
        
        return fed != null;
        
    }
    
    synchronized public void disconnect(boolean immediateShutdown) {
        
        if (fed != null) {

            if(immediateShutdown) {
                
                fed.shutdownNow();
                
            } else {
                
                fed.shutdown();
                
            }
            
        }
        
        fed = null;

    }
    
    synchronized public IBigdataFederation getFederation() {

        if (fed == null) {

            throw new IllegalStateException();

        }

        return fed;

    }

    synchronized public IBigdataFederation connect() {

        if (fed == null) {

            fed = new EmbeddedFederation(this);

        }

        return fed;

    }

    /**
     * Options for the embedded (in process) federation. Service instances will
     * share the same configuration properties except for the name of the
     * backing store file.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends MetadataService.Options {

        /**
         * The name of the optional property whose value is the #of data
         * services that will be (re-)started.
         */
        public static String NDATA_SERVICES = "ndataServices";

        /**
         * The default is two (2).
         */
        public static String DEFAULT_NDATA_SERVICES = "2";

        /**
         * <code>data.dir</code> - The name of the required property whose
         * value is the name of the directory under which each metadata and data
         * service will store their state (their journals and index segments).
         * <p>
         * Note: A set of subdirectories will be created under the specified
         * data directory. Those subdirectories will be named by the UUID of the
         * corresponding metadata or data service. In addition, the subdirectory
         * corresponding to the metadata service will have a file named ".mds"
         * to differentiate it from the data service directories. The presence
         * of that ".mds" file is used to re-start the service as a metadata
         * service rather than a data service.
         * <p>
         * Since a set of subdirectories will be created for the embedded
         * federation, it is important to give each embedded federation its own
         * data directory. Otherwise a new federation starting up with another
         * federation's data directory will attempt to re-start that federation.
         */
        public static final String DATA_DIR = "data.dir";

    }

}
