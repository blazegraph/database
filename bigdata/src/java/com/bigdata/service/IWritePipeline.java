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
 * Created on Mar 18, 2007
 */

package com.bigdata.service;

import java.util.UUID;

import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.IUpdateStore;

/**
 * Replicates writes against a primary store by pipelining them onto one or more
 * secondary stores in order to provide high availability and failover.
 * Replication is handled at the level of the {@link IRawStore} and
 * {@link IUpdateStore} APIs so that it may be tested in a local environment and
 * even used to provide media failover. However, the primary use case is state
 * replication, high availability, and failover for {@link DataService}s.
 * <p>
 * The write pipeline operates at the {@link IRawStore} API. All writes are
 * directed to the primary. It simply streams writes down to the next service in
 * the pipeline and that is the sole way in which writes are made on the
 * secondaries. The result is that the secondaries are a bitwise replication of
 * the primary store state. Secondaries must catch up no later than the next
 * commit point. This allows writes on the pipeline to be asynchronous with
 * respect to the writes on the primary (the application does not wait for the
 * secondaries when writing on a store), but synchronous with respect to commit
 * points on the primary. Internally this can be handled by queuing writes and
 * blocking until the queue is empty at each commit point. Writes need not be
 * ordered as long as commit point synchronization is maintained.
 * <p>
 * Concurrency control for writes and commit processing are solely handled by
 * the primary. If a secondary fails, then it is taken out of the pipeline. In
 * particular, an error when writing on a secondary will not cause a commit to
 * fail.
 * <p>
 * Since secondaries are bitwise replications of the primary as of the most
 * recent commit point, they are fully able to respond to requests for
 * historical data. This provides high-availability by allowing read-committed
 * and read-historical requests to be handled by secondaries in order to reduce
 * the load on the primary.
 * <p>
 * At the {@link DataService} layer, replication requires not only replicating
 * the state of the live journal, but also replicating the state of the managed
 * resources (and the release) of those resources.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo we need a logical / physical distinction for data services. the
 *       physical data service is a JINI service and has a JINI ServiceUUID. the
 *       logical data service is a pipeline of physical data services. each
 *       secondary data service is a faithful replicate of the corresponding
 *       primary data service as of the most recent commit point.
 *       <p>
 *       The metadata index stores <em>logical</em> data service identifiers,
 *       which are {@link UUID}s. This is necessary since many index partitions
 *       can be mapped onto the logical data service and we do not want to
 *       update all of those records if the failover pipeline is changed.
 *       <p>
 *       When the logical data service identifier is resolved to a physical data
 *       service proxy we need to (a) always choose the primary if the operation
 *       is a write; (b) share the read committed and historical read load out
 *       over the secondaries; and (c) handle RMI IO errors by failing over to
 *       (another) secondary.
 * 
 * @todo Recognize errors that are transients (route to host problems that do
 *       not indicate a failed data service), that are application problems
 *       (e.g., a class that is not serializable), and that indicate service
 *       death.
 * 
 * @todo Resolve a logical data service UUID to a physical data service. This
 *       can be done easily enough by a variant of
 *       com.bigdata.service.jini.ServiceCache that accepts a logical data
 *       service UUID and returns a ServiceItem[] that is a snapshot of the
 *       known physical data services for that logical data service.
 *       <p>
 *       The clients information can be stale and in any case does not include
 *       which physical data service is the master (primary). For read committed
 *       and historical reads, the client should choose the physical data at
 *       random among those provided. If the client requires the master, then it
 *       should choose a physical data service from among those provided at
 *       random and query it for the identity (ServiceUUID) of the master. The
 *       client will store the ServiceUUID of the master in a secondary (or
 *       outer) cache that permits fast resolution of the master for a logical
 *       data service. The clients information regarding the secondaries and the
 *       primary will remain valid until a service "leaves" (dies).
 * 
 * @todo Robust selection of a new primary if a primary dies (paxos territory -
 *       zoo keeper?)
 * 
 * @todo Robust recruitment of secondaries to maintain replication levels.
 * 
 * @todo Robustness of applications during transients, especially when the
 *       primary is not responding and no secondary has been nominated as the
 *       new primary.
 */
public interface IWritePipeline { //extends IRawStore, IUpdateStore {

//    /**
//     * Write a record onto the pipeline (useful for the primary when a write is
//     * made on the journal since the record is fully materialized).
//     * 
//     * @param addr
//     *            The address at which the record is written.
//     * @param data
//     *            The record.
//     */
//    public void write(long addr,byte[] data);
//    
//    /**
//     * Stream a record onto the pipeline (useful for the secondary to stream
//     * further downstream).
//     * 
//     * @param addr
//     *            The address at which the record is written.
//     * @param is
//     *            The data is read from this stream.
//     */
//    public void write(long addr,InputStream is);
//    
//    /**
//     * Stream a file onto the pipeline (useful for replicating file system data
//     * on the pipeline).
//     * 
//     * @param file
//     *            The file name.
//     * @param is
//     *            The file contents are read from this stream.
//     */
//    public void write(File file,InputStream is);
//    
//    /**
//     * The pipeline of data replication services. The location of this service
//     * in the pipeline may be found by scanning the returned UUIDs.
//     * 
//     * @return
//     */
//    public UUID[] getPipline();
    
}
