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
 * Created on Feb 19, 2008
 */

package com.bigdata.journal;

import java.io.File;
import java.util.UUID;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentFileStore;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;

/**
 * Interface manging the resources on which indices are stored. The resources
 * may be either a journal, which contains {@link BTree}s, or an
 * {@link IndexSegmentFileStore} containing a single {@link IndexSegment}.
 * <p>
 * Note: For historical reasons there are two implementations of this interface.
 * The {@link Journal} uses an implementation that does not support
 * {@link #overflow()} since the managed "journal" is always the actual
 * {@link Journal} reference. The {@link ResourceManager} supports
 * {@link #overflow()} and decouples the management of the journal and index
 * segment resources and the concurrency control from the journal itself.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo update javadoc.
 */
public interface IResourceManager {

    /**
     * Normal shutdown.
     * <p>
     * Note: Implementations MUST {@link IConcurrencyManager#shutdown()} the
     * {@link IConcurrencyManager} coordinating access to the resources before
     * shutting down the {@link IResourceManager}.
     */
    public void shutdown();

    /**
     * Immediate shutdown.
     */
    public void shutdownNow();

    /**
     * The journal on which writes are made.  This is updated by {@link #overflow()}.
     */
    public AbstractJournal getLiveJournal();

    /**
     * The directory for temporary files.
     * 
     * @see Options#TMP_DIR
     */
    public File getTmpDir();

    /**
     * The directory for managed resources.
     * 
     * @see com.bigdata.journal.ResourceManager.Options#DATA_DIR
     */
    public File getDataDir();
    
    /**
     * Return the reference to the journal which has the most current data for
     * the given timestamp. If necessary, the journal will be opened.
     * 
     * @param timestamp
     *            The startTime of an active transaction, <code>0L</code> for
     *            the current unisolated index view, or <code>-timestamp</code>
     *            for a historical view no later than the specified timestamp.
     * 
     * @return The corresponding journal for that timestamp -or-
     *         <code>null</code> if no journal has data for that timestamp,
     *         including when a historical journal with data for that timestamp
     *         has been deleted.
     */
    public AbstractJournal getJournal(long timestamp);
    
    /**
     * Opens an {@link IRawStore}.
     * 
     * @param uuid
     *            The UUID identifying that store file.
     * 
     * @return The open {@link IRawStore}.
     * 
     * @throws RuntimeException
     *             if something goes wrong.
     */
    public IRawStore openStore(UUID uuid);
    
    /**
     * Return the ordered {@link AbstractBTree} sources for an index or a view
     * of an index partition. The {@link AbstractBTree}s are ordered from the
     * most recent to the oldest and together comprise a coherent view of an
     * index partition.
     * 
     * @param name
     *            The name of the index.
     * @param timestamp
     *            Either the startTime of an active transaction,
     *            {@link ITx#UNISOLATED} for the current unisolated index view,
     *            {@link ITx#READ_COMMITTED} for a read-committed view, or
     *            <code>-timestamp</code> for a historical view no later than
     *            the specified timestamp.
     * 
     * @return The sources for the index view or <code>null</code> if the
     *         index was not defined as of the timestamp.
     * 
     * @see FusedView
     */
    public AbstractBTree[] getIndexSources(String name, long timestamp);

    /**
     * Return the named index as of the specified timestamp.
     * 
     * @param name
     *            The index name.
     * @param timestamp
     *            Either the startTime of an active transaction,
     *            {@link ITx#UNISOLATED} for the current unisolated index view,
     *            {@link ITx#READ_COMMITTED} for a read-committed view, or
     *            <code>-timestamp</code> for a historical view no later than
     *            the specified timestamp.
     * 
     * @return The index or <code>null</code> iff there is no index registered
     *         with that name for that <i>timestamp</i>, including if the
     *         timestamp is a transaction identifier and the transaction is
     *         unknown or not active.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>
     */
//    * 
//    * @exception IllegalStateException
//    *                if <i>timestamp</i> is positive but there is no active
//    *                transaction with that timestamp.
    public IIndex getIndex(String name, long timestamp);

    /**
     * Return statistics about the named index. This method will report on all
     * resources supporting the view of the index as of the specified timestamp.
     * 
     * @param name
     *            The index name.
     * @param timestamp
     *            Either the startTime of an active transaction,
     *            {@link ITx#UNISOLATED} for the current unisolated index view,
     *            {@link ITx#READ_COMMITTED} for a read-committed view, or
     *            <code>-timestamp</code> for a historical view no later than
     *            the specified timestamp.
     * 
     * @return Statistics about that index -or- <code>null</code> if the index
     *         is not defined as of that timestamp.
     * 
     * @todo report as XML or Object.
     */
    public String getStatistics(String name, long timestamp);
    
    /**
     * Statistics about the {@link IResourceManager}.
     * 
     * @todo report as XML or Object.
     */
    public String getStatistics();
    
    /**
     * Creates a new journal migrate the named indices to the new journal.
     * Typically this involves updating the view of the named indices such that
     * they read from a fused view of an empty index on the new journal and the
     * last committed state of the index on the old journal.
     * <p>
     * Note: When this method returns <code>true</code> journal references
     * MUST NOT be presumed to survive this method. In particular, the old
     * journal MAY be closed out by this method and marked as read-only
     * henceforth.
     * 
     * @param exclusiveLock
     *            <code>true</code> iff the current thread has an exclusive
     *            lock on the journal.
     * 
     * @param writeService
     *            The service on which {@link ITx#UNISOLATED} tasks are executed
     *            and which is responsible for handling commits.
     * 
     * @return true iff the journal was replaced by a new journal.
     * 
     * @see #getLiveJournal()
     */
    public boolean overflow(boolean exclusiveLock,WriteExecutorService writeService);
    
    /**
     * Deletes all resources.
     * 
     * @exception IllegalStateException
     *                if the {@link IResourceManager} is open.
     */
    public void destroyAllResources();

    /**
     * Deletes all resources having no data for the specified timestamp. This
     * method is used by the {@link ITransactionManager} to request the release
     * of old resources as the last possible historical commit state which can
     * be read gets slide forward in time. The resource manager MAY ignore or
     * queue the request if it would conflict with read locks required for any
     * other reasons, e.g., the resource manager's own resource retention
     * policy. Ignoring some requests generally has little impact since the
     * transaction manager can be relied on to notify the resource manager of a
     * new "delete" point as more transactions commit.
     * <p>
     * Note: The ability to read from a historical commit point requires the
     * existence of the journals back until the one covering that historical
     * commit point. This is because the distinct historical commit points for
     * the indices are ONLY defined on the journals. The index segments carry
     * forward the commit state of a specific index as of the commitTime of the
     * index from which the segment was built. This means that you can
     * substitute the index segment for the historical index state on older
     * journals, but the index segment carries forward only a single commit
     * state for the index so it can not be used to read from arbitrary
     * historical commit points.
     * 
     * @param timestamp
     *            The timestamp.
     * 
     * @todo the precise semantics of this method have not been nailed down yet
     *       as the coordination between the resource manager and the
     *       transaction manager has not been worked out in detail. One of the
     *       issues is how to impose a resource release policy when distributed
     *       transactions are not in use.
     */
    public void releaseOldResources(long timestamp);
    
    /**
     * Return the file on which a new {@link IndexSegment} should be written.
     * The file will exist but will have zero length.
     * 
     * @param indexMetadata
     *            The index metadata.
     * 
     * @return The file.
     */
    public File getIndexSegmentFile(IndexMetadata indexMetadata);

    /**
     * Return the {@link IMetadataService} that will be used to assign partition
     * identifiers and which will be notified when index partitions are splits,
     * moved, etc.
     */
    public IMetadataService getMetadataService();
    
    /**
     * Return the {@link UUID} of the {@link IDataService} whose resources are
     * being managed.
     */
    public UUID getDataServiceUUID();

    /**
     * Return the {@link UUID}[] of the {@link IDataService} failover chain for
     * the {@link IDataService} resources are being managed.
     */
    public UUID[] getDataServiceUUIDs();

}
