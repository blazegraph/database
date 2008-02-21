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

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentFileStore;

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
     * Creates a new journal and re-defines the views for all named indices to
     * include the pre-overflow view with reads being absorbed by a new btree on
     * the new journal.
     * <p>
     * Note: This method MUST NOT be invoked unless you have exclusive access to
     * the journal. Typically, the only time you can guarentee this is during
     * commit processing. Therefore overflow is generally performed by a
     * post-commit event handler.
     * <p>
     * Note: Journal references MUST NOT be presumed to survive this method. In
     * particular, the old journal will be closed out by this method and marked
     * as read-only henceforth.
     * <p>
     * Note: The decision to simply re-define views rather than export the data
     * on the named indices on the old journal
     * 
     * @todo change the return type to void and clean up
     *       {@link AbstractJournal#overflow()} and the various things that were
     *       designed to trigger that method. Overflow should only be invoked by
     *       the {@link IResourceManager} in a "post-commit" operation where it
     *       notices that the overflow criteria have been satisified.
     */
    public boolean overflow();
    
    /**
     * Deletes all resources.
     * 
     * @exception IllegalStateException
     *                if the {@link IResourceManager} is open.
     */
    public void delete();

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
    public void delete(long timestamp);
    
}
