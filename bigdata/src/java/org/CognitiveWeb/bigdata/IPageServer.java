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

/**
 * <p>
 * A page server is a peer component used by clients and servers alike to access
 * pages in the distributed database. A page server provides service for one and
 * only one database. Page servers only communicate directly with page servers
 * for the same database.
 * </p>
 * <h3>Client Configuration</h3>
 * <p>
 * A client configuration typically uses a local page cache, obtains locks as
 * required for the segments that it needs to access, and distributes requests
 * to page servers fronting for the segments containing pages of interest.
 * Clients may optionally coordinate to provide a shared page cache in order to
 * reduce the burden on servers. A client may optionally store some segments
 * locally, in which case preference for read operations is given to the local
 * copy of the segment.
 * </p>
 * <h3>Server Configuration</h3>
 * <p>
 * A server configuration generally stores 100s or 1000s of segments locally and
 * makes the pages in those segments available to database clients. If a server
 * dies the client will query the catalog for another server for the same
 * segment. If a server sheds a segment or sheds clients for a segment in order
 * to reduce its overall load, then the clients will query the catalog for
 * another server for the same segment. A server for a non-replicated segment
 * may not shed the workload for that segment, but it may request replication by
 * another server with a smaller workload.
 * </p>
 * <p>
 * A server configuration does cache pages since segment servers are expected to
 * provide their own page cache mechanisms.
 * </p>
 * <h3>Lock Server Interaction</h3>
 * <p>
 * At startup a page server discovers the lock servers for that database (there
 * should be only one). If the page server is serving local segments, it
 * notifies the catalog service that there are copies of those segments on the
 * host running the page server. Those enties are then marked "live" in the
 * catalog.
 * </p>
 * <p>
 * Locks may be explicitly requested and are automatically obtained when a page
 * is accessed in a segment for which a transaction does not hold a lock. Locks
 * are normally released by either a commit or an abort of the transaction, but
 * locks may be optionally held by a commit.
 * </p>
 * <h3>Non-blocking I/O</h3>
 * <p>
 * This interface specifies the semantics of a synchronous API for page servers.
 * Communications with local segments, the client cache sharing protocol, and
 * the page transport protocol for communications with remote page servers all
 * use non-blocking I/O.
 * </p>
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 */

public interface IPageServer
{
    
    /**
     * Obtain a lock on the segment containing the specified object (row)
     * 
     * @param tx
     *            The transaction identifier.
     * @param oid
     *            The object (row) identifier. The page and slot components are
     *            ignored and may be zero. The lock is obtained for the
     *            specified segment in the specified partition of the database.
     * @param readOnly
     *            True if a read-only lock is required. False if a write lock is
     *            required.
     * @param timeout
     *            The maximum time to wait in milliseconds for the lock or zero
     *            (0) to wait forever.
     * @param RuntimeException
     *            if the lock could not be granted within the alloted time.
     * @param RuntimeException
     *            if a deadlock would result.
     */
    public void lock(long tx,OId oid, boolean readOnly,long timeout);
    
    /**
     * Send a page of data.
     * 
     * @param pageId
     *            The page identifier. This is a object identifier whose slot
     *            component is ignored and may be zero(0).
     * @param data
     *            The data.
     * 
     * @throws IOException
     */
    public void write(long tx, OId pageId, byte[] data ) throws IOException;
    
    /**
     * Read a page of data.
     * 
     * @param pageId
     *            The page identifier. This is an object identifier (oid) whose
     *            slot component is ignored and may be zero(0).
     * 
     * @return The data.
     * 
     * @throws IOException
     */
    public byte[] read(long tx, OId pageId ) throws IOException;

    /**
     * Prepare all segments for a commit.
     */
    public void prepare(long tx);

    /**
     * Commit the transaction.
     * 
     * @param tx
     *            The transaction identifier.
     * @param releaseLocks
     *            When true, the locks held by the transaction will be release.
     * @param syncToDisk
     *            When true, the commit will force changes to stable storage.
     *            Forcing to stable storage is the slowest operation in a
     *            commit, but it means that the data are "safe" on disk.
     *            Replicated segments with redundent power sources provide an
     *            alternative strategy for data safety. As long as there is at
     *            least one copy of each segment surviving your data is safe.
     * 
     * @todo Have servers sync to disk if they become decoupled from the
     *       network.
     */
    public void commit(long tx, boolean releaseLocks, boolean syncToDisk);
    
    /**
     * Abort the transaction.
     */
    public void abort(long tx);

//    /**
//     * Close the connection to the page server.
//     */
//    public void close();
    
}
