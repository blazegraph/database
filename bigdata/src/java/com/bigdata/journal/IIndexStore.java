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
package com.bigdata.journal;

import java.util.concurrent.ExecutorService;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.IIndex;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.sparse.GlobalRowStoreSchema;
import com.bigdata.sparse.SparseRowStore;

/**
 * Interface accessing named indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IIndexStore {

    /**
     * Return a read-only view of the named index as of the specified timestamp.
     * 
     * @param name
     *            The index name.
     * @param timestamp
     *            The timestamp.
     * 
     * @return The index or <code>null</code> iff there is no index registered
     *         with that name for that timestamp.
     */
    public IIndex getIndex(String name, long timestamp);

    /**
     * Return the global {@link SparseRowStore} used to store named property
     * sets.
     * <p>
     * The {@link SparseRowStore} only permits {@link ITx#UNISOLATED} writes, so
     * you MUST specify {@link ITx#UNISOLATED} as the timestamp if you intend to
     * write on the global row store!
     * <p>
     * You can request the most recent committed state of the global row store
     * by specifying {@link ITx#READ_COMMITTED}. 
     * 
     * @param timestamp
     *            The timestamp of the view.
     * 
     * @see GlobalRowStoreSchema
     */
    public SparseRowStore getGlobalRowStore(long timestamp);
    
    /**
     * Return the global file system used to store block-structured files and
     * their metadata and as a source and sink for map/reduce processing.
     * 
     * @see BigdataFileSystem
     */
    public BigdataFileSystem getGlobalFileSystem();
    
    /**
     * A factory for {@link TemporaryStore}s. {@link TemporaryStore}s are
     * thread-safe and may be used by multiple processes at once. Old
     * {@link TemporaryStore}s are eventually retired by the factory and their
     * storage is reclaimed once they are finalized (after they are no longer in
     * use by any process). The decision to retire a {@link TemporaryStore} is
     * either made implicitly, when it is no longer weakly reachable, or
     * explicitly, when it has grown large enough that no new processes should
     * begin using that {@link TemporaryStore}. In the latter case, the
     * {@link TemporaryStore} will remain available to the process(es) using it
     * and a new {@link TemporaryStore} will be allocated and made available to
     * the caller.
     * <p>
     * It is important that processes do not hold a hard reference to a
     * {@link TemporaryStore} beyond the end of the process as that will prevent
     * the {@link TemporaryStore} from being finalized. Holding reference to an
     * {@link AbstractBTree} created on a {@link TemporaryStore} is equivilent
     * to holding a hard reference to the {@link TemporaryStore} itself since
     * the {@link AbstractBTree} holds onto the backing {@link IRawStore} using
     * a hard reference.
     * 
     * @return A {@link TemporaryStore}.
     */
    public TemporaryStore getTempStore();
    
    /**
     * Return the default locator for resources that are logical index
     * containers (relations and relation containers).
     */
    public IResourceLocator getResourceLocator();

    /**
     * A {@link ExecutorService} that may be used to parallelize operations.
     * This service is automatically used when materializing located resources.
     * While the service does not impose concurrency controls, tasks run on this
     * service may submit operations to a {@link ConcurrencyManager}.
     */
    public ExecutorService getExecutorService();

    /**
     * The service that may be used to acquire exclusive or shared locks for a
     * resource hierarchy. This is used primarily when creating or destroying
     * {@link IRelation}s in order to make those operations atomic, but the
     * service may be used for any resource whether or not they correspond to
     * the global index namespace.
     */
    public IResourceLockService getResourceLockService();

    /**
     * The database wide timestamp of the most recent commit on the store or 0L
     * iff there have been no commits. In a local database, this timestamp is
     * generated by a local timestamp service. In a distributed database, this
     * timestamp is generated by a shared timestamp service. The timestamps
     * returned by this method are strictly increasing for a given store and for
     * a given database.
     * <p>
     * This method is useful if you plan to issue a series of read-consistent
     * requests against the most current commit point of the database.
     * 
     * @return The timestamp of the most recent commit on the store or 0L iff
     *         there have been no commits.
     * 
     * @see IRootBlockView#getLastCommitTime()
     */
    public long getLastCommitTime();
    
}
