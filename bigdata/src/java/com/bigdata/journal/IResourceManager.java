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
import java.util.concurrent.Future;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.counters.CounterSet;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.resources.StaleLocatorReason;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.IServiceShutdown;

/**
 * Interface manging the resources on which indices are stored. The resources
 * may be either a journal, which contains {@link BTree}s, or an
 * {@link IndexSegmentStore} containing a single {@link IndexSegment}.
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
 */
public interface IResourceManager extends IServiceShutdown {

    /**
     * The directory for temporary files.
     * 
     * @see Options#TMP_DIR
     */
    public File getTmpDir();

    /**
     * The directory for managed resources.
     * 
     * @see com.bigdata.resources.ResourceManager.Options#DATA_DIR
     */
    public File getDataDir();
    
    /**
     * The journal on which writes are made.  This is updated by {@link #overflow()}.
     */
    public AbstractJournal getLiveJournal();

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
     * <p>
     * Note: Index sources loaded from a historical timestamp (vs the live
     * unisolated index view) will always be read-only.
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
     * Examine the partition metadata (if any) for the {@link BTree}. If the
     * {@link BTree} is part of an index partition, then return all of the
     * sources for a view of that index partition (the {@link BTree} will be the
     * first element in the array and, if there are no more sources for the
     * index partition, then it will also be the sole element of the array).
     * Otherwise return an array consisting of a single element, which is the
     * {@link BTree}.
     * 
     * @param btree
     *            A {@link BTree}.
     * 
     * @return The source(s) for the view associated with that {@link BTree}.
     */
    public AbstractBTree[] getIndexSources(String name, long timestamp, BTree btree);
    
    /**
     * Return a view of the named index as of the specified timestamp.
     * <p>
     * Note: An index view loaded from a historical timestamp (vs the live
     * unisolated index view) will always be read-only.
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
     * 
     * @exception StaleLocatorException
     *                if the named index does not exist at the time the
     *                operation is executed and the {@link IResourceManager} has
     *                information which indicates that the index partition has
     *                been split, joined or moved.
     * 
     * @see IIndexStore#getIndex(String, long)
     */
    public IIndex getIndex(String name, long timestamp);

    /**
     * Return non-<code>null</code> iff <i>name</i> is the name of an
     * {@link ITx#UNISOLATED} index partition that was located on the associated
     * {@link DataService} but which is now gone.
     * 
     * @param name
     *            The name of an index partition.
     * 
     * @return The reason (split, join, or move) -or- <code>null</code> iff
     *         the index partition is not known to be gone.
     */
    public StaleLocatorReason getIndexPartitionGone(String name);

    /**
     * Statistics about the {@link IResourceManager}.
     */
    public CounterSet getCounters();
    
    /**
     * Return <code>true</code> if the pre-conditions for overflow of the
     * {@link #getLiveJournal() live journal} have been met. In general, this
     * means that the live journal is within some threshold of the configured
     * {@link Options#MAXIMUM_EXTENT}.
     * 
     * @return <code>true</code> if overflow processing should occur.
     */
    public boolean shouldOverflow();
    
    /**
     * <code>true</code> if overflow processing is enabled and
     * <code>false</code> if overflow processing was disabled as a
     * configuration option, in which case overflow processing can
     * not be performed.
     */
    public boolean isOverflowEnabled();
    
    /**
     * Overflow processing creates a new journal, migrates the named indices on
     * the current journal the new journal, and continues operations on the new
     * journal. Typically this involves updating the view of the named indices
     * such that they read from a fused view of an empty index on the new
     * journal and the last committed state of the index on the old journal.
     * <p>
     * Note: When this method returns <code>true</code> journal references
     * MUST NOT be presumed to survive this method. In particular, the old
     * journal MAY be closed out by this method and marked as read-only
     * henceforth.
     * <p>
     * Note: The caller MUST ensure that they have an exclusive lock on the
     * {@link WriteExecutorService} such that no task is running with write
     * access to the {@link #getLiveJournal() live journal}.
     * <p>
     * Note: The implementation MUST NOT write on the old journal - those writes
     * will not be made restart safe by the {@link WriteExecutorService} - but
     * it MAY write on the new journal.
     * 
     * @return The {@link Future} for the task handling post-processing of the
     *         old journal.
     */
    public Future<Object> overflow();
    
    /**
     * Deletes all resources.
     * 
     * @exception IllegalStateException
     *                if the {@link IResourceManager} is open.
     */
    public void deleteResources();

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
     * Return the {@link UUID} of the {@link IDataService} whose resources are
     * being managed.
     */
    public UUID getDataServiceUUID();
    
    /**
     * The local {@link DataService} whose resources are being managed.
     * 
     * @throws UnsupportedOperationException
     *             if the {@link IResourceManager} is not part of an
     *             {@link IBigdataFederation}.
     */
    public DataService getDataService();
    
    /**
     * The federation whose resources are being managed.
     * 
     * @throws UnsupportedOperationException
     *             if the {@link IResourceManager} is not part of an
     *             {@link IBigdataFederation}.
     */
    public IBigdataFederation getFederation();
    
    /**
     * Return the ordered {@link UUID}[] of the physical {@link IDataService}
     * failover chain for the logical {@link IDataService} whose resources are
     * under management by this class.
     * 
     * @todo not used right now.  might go away.
     */
    public UUID[] getDataServiceUUIDs();

}
