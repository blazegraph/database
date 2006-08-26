/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Jun 18, 2006
 */
package org.CognitiveWeb.bigdata;

import java.io.IOException;
import java.util.Properties;
import java.util.WeakHashMap;

/**
 * A page server.
 * 
 * @todo Write javadoc.
 * 
 * @todo The {@link IPageServer}interface is defined in terms of synchronous
 *       operations. How can the interface be best broken down to exploit an
 *       asynchronous non-blocking I/O page transport and locking protocol while
 *       being rolled up to synchronous semantics for use by the application
 *       layer? This probably needs to be encapsulated in the application facing
 *       layer of the page cache. That should fit over a p2p page cache
 *       transport layer (shared cache among clients), which in turn sits over
 *       the local page server. The local page server may or may not have local
 *       segments. Requests for non-local segments are directed to a remote page
 *       server for that segment. Remote servers are discovered using the
 *       catalog service, which makes recommendations based on load. Clients may
 *       be redirected to other copies of the same segment based on evolving
 *       load characteristics.
 * 
 * @todo Locks are acquired using the lock server. Where does that happen in the
 *       client stack?
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 */
public class PageServer implements IPageServer
{

    /**
     * The name of the database.
     */
    final private String _database;
    
    public static class RuntimeOptions {
        
        /**
         * The name of the optional property whose value is the name of the
         * distributed database. The lock server and catalog for the named
         * database will be discovered using jini.  When not specified, the
         * value defaults to {@link #DATABASE_DEFAULT}.
         */
        public static final String DATABASE = "bigdata.database";

        /**
         * The name of the default database.
         */
        public static final String DATABASE_DEFAULT = "default-database";
        
        /**
         * The name of the optional property whose value is the name of an XML
         * configuration file listing the locally available segments. This is
         * used by the page server to locate and start up local segments.  The
         * named file is created if it does not exist.
         */
        public static final String SEGMENT_CONFIG_FILE = "bigdata.segmentConfigFile";

        /**
         * The name of the segment configuration file.
         */
        public static final String SEGMENT_CONFIG_FILE_DEFAULT = "segments.xml";
        
        /**
         * <p>
         * The name of the boolean property indicating whether the page server
         * is a client or a server. The default is "true" indicating that the
         * page server is a server.
         * </p>
         * <p>
         * When false, the page server is a client only. Clients do not
         * advertise local segments for access by other servers. A client page
         * server provides a connection to the named database and is shared by a
         * JVM.
         * </p>
         */
        public static final String SERVER = "bigdata.server";
        
        public static final String SERVER_DEFAULT = "true";
        
    }

    //
    // Constructor and factory.
    //
    
    /**
     * Start a page server.
     * 
     * @param properties
     *            The configuration options.
     */
    protected PageServer(Properties properties) {
        _database = properties.getProperty(RuntimeOptions.DATABASE,
                RuntimeOptions.DATABASE_DEFAULT);
        if( _database.equals("") ) {
            throw new IllegalArgumentException();
        }
        // @todo start service and register it.
    }

    static private WeakHashMap _instances = new WeakHashMap();
    
    /**
     * Factory for page servers. If an existing page server is found for the
     * named database then it is returned. Otherwise a page server is created
     * for the named database using the provided properties.
     * 
     * @param properties
     *            Page server properties.
     * 
     * @return The configured and possibly pre-existing page server.
     */

    /**
     * The name of the database.
     */
    public String getDatabase() {
        return _database;
    }
    
    /**
     * True iff the lock server and catalog for the named database could be
     * resolved.
     * 
     * @return
     */
    public boolean isConnected() {
        throw new UnsupportedOperationException();
    }
    
    public void lock(long tx,OId oid, boolean readOnly,long timeout) {
        throw new UnsupportedOperationException();
    }

    public void write(long tx, OId pageId, byte[] data ) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    public byte[] read(long tx, OId pageId ) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void prepare(long tx) {
        throw new UnsupportedOperationException();
    }
    
    public void commit(long tx, boolean releaseLocks, boolean syncToDisk) {
        throw new UnsupportedOperationException();
    }
    
    public void abort(long tx) {
        throw new UnsupportedOperationException();
    }

}
