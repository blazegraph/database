/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.btree;

import com.bigdata.btree.view.FusedView;
import com.bigdata.counters.ICounterSetAccess;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IAtomicStore;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.Name2Addr;
import com.bigdata.journal.Name2Addr.Entry;

/**
 * Interface in support of the {@link Checkpoint} record protocol.
 * 
 * @author thompsonbry@users.sourceforge.net
 * 
 *         TODO Try to lift out an abstract implementation of this interface for
 *         HTree, BTree, and Stream. This will be another step towards GIST
 *         support. There are protected methods which are used on those classes
 *         which should be lifted into the abstract base class. Also, try to
 *         reconcile this interface with {@link ILocalBTreeView} implementations
 *         that do not implement {@link ICheckpointProtocol} ({@link FusedView},
 *         {@link ReadCommittedView}). 
 *         
 * @see <a href="http://trac.bigdata.com/ticket/585" > GIST </a>
  */
public interface ICheckpointProtocol extends ICommitter, ICounterSetAccess,
        ISimpleIndexAccess, IReadWriteLockManager {

	/**
	 * The value of the record version number that will be assigned to the next
	 * node or leaf written onto the backing store. This number is incremented
	 * each time a node or leaf is written onto the backing store. The initial
	 * value is ZERO (0). The first value assigned to a node or leaf will be
	 * ZERO (0).
	 * 
	 * TODO Nobody is actually incrementing this value right now.
	 */
	public long getRecordVersion();

	/**
	 * Returns the most recent {@link ICheckpoint} record.
	 * 
	 * @return The most recent {@link ICheckpoint} record and never
	 *         <code>null</code>.
	 */
    public ICheckpoint getCheckpoint();
    
	/**
	 * The address at which the most recent {@link IndexMetadata} record was
	 * written.
	 */
    public long getMetadataAddr();
    
    /**
     * The address of the last written root of the persistent data structure
     * -or- <code>0L</code> if there is no root. A <code>0L</code> return may be
     * an indication that an empty data structure will be created on demand.
     */
    public long getRootAddr();
    
	/**
	 * Sets the lastCommitTime.
	 * <p>
	 * Note: The lastCommitTime is set by a combination of the
	 * {@link AbstractJournal} and {@link Name2Addr} based on the actual
	 * commitTime of the commit during which an {@link Entry} for that index was
	 * last committed. It is set for both historical index reads and unisolated
	 * index reads using {@link Entry#commitTime}. The lastCommitTime for an
	 * unisolated index will advance as commits are performed with that index.
	 * 
	 * @param lastCommitTime
	 *            The timestamp of the last committed state of this index.
	 * 
	 * @throws IllegalArgumentException
	 *             if lastCommitTime is ZERO (0).
	 * @throws IllegalStateException
	 *             if the timestamp is less than the previous value (it is
	 *             permitted to advance but not to go backwards).
	 */
    public void setLastCommitTime(final long lastCommitTime);
    
    /**
     * The timestamp associated with the last {@link IAtomicStore#commit()} in
     * which writes buffered by this index were made restart-safe on the backing
     * store. The lastCommitTime is set when the index is loaded from the
     * backing store and updated after each commit. It is ZERO (0L) when an
     * index is first created and will remain ZERO (0L) until the index is
     * committed. If the backing store does not support atomic commits, then
     * this value will always be ZERO (0L).
     */
    public long getLastCommitTime();
    
	/**
	 * Checkpoint operation must {@link #flush()} dirty nodes, dirty persistent
	 * data structures, etc, write a new {@link Checkpoint} record on the
	 * backing store, save a reference to the current {@link Checkpoint}, and
	 * return the address of that {@link Checkpoint} record.
	 * <p>
	 * Note: A checkpoint by itself is NOT an atomic commit. The commit protocol
	 * is at the store level and uses {@link Checkpoint}s to ensure that the
	 * state of the persistence capable data structure is current on the backing
	 * store.
	 * 
	 * @return The address at which the {@link Checkpoint} record for the
	 *         persistence capable was written onto the store. The data
	 *         structure can be reloaded from this {@link Checkpoint} record.
	 */
    public long writeCheckpoint();
    
    /**
     * Checkpoint operation must {@link #flush()} dirty nodes, dirty persistent
     * data structures, etc, write a new {@link Checkpoint} record on the
     * backing store, save a reference to the current {@link Checkpoint}, and
     * return the address of that {@link Checkpoint} record.
     * <p>
     * Note: A checkpoint by itself is NOT an atomic commit. The commit protocol
     * is at the store level and uses {@link Checkpoint}s to ensure that the
     * state of the persistence capable data structure is current on the backing
     * store.
     * 
     * @return The {@link Checkpoint} record for the persistent data structure
     *         which was written onto the store. The persistent data structure
     *         can be reloaded from this {@link Checkpoint} record.
     */
    public Checkpoint writeCheckpoint2();
    
    /**
     * Return the {@link IDirtyListener}.
     */
    public IDirtyListener getDirtyListener();

    /**
     * Set or clear the listener (there can be only one).
     * 
     * @param listener The listener.
     */
    public void setDirtyListener(final IDirtyListener listener) ;
    
	/**
	 * The metadata for the index. This is full of good stuff about the index.
	 * <p>
	 * Note: The same method is also declared by {@link IIndex} in order to
	 * provide access to the {@link IndexMetadata} for remote clients in
	 * scale-out.
	 * 
	 * @see IIndex#getIndexMetadata()
	 */
	public IndexMetadata getIndexMetadata();

//	/*
//	 * Generic data access methods defined for all persistence capable 
//	 * data structures.
//	 */
//
//    /**
//     * Return the #of entries in the index.
//     * <p>
//     * Note: If the index supports deletion markers then the range count will be
//     * an upper bound and may double count tuples which have been overwritten,
//     * including the special case where the overwrite is a delete.
//     * 
//     * @return The #of tuples in the index.
//     * 
//     * @see IRangeQuery#rangeCount()
//     */
//    public long rangeCount();
//
//    /**
//     * Visit all entries in the index in the natural order of the index
//     * (dereferencing visited tuples to the application objects stored within
//     * those tuples).
//     */
//    public ICloseableIterator<?> scan();
//
//    /**
//     * Remove all entries in the index.
//     */
//    public void removeAll();

    /*
     * reopen() / close() protocol
     */
    
    /**
     * (Re-) open the index. This method is part of a {@link #close()} /
     * {@link #reopen()} protocol. That protocol may be used to reduce the
     * resource burden of an index. This method is automatically invoked by a
     * variety of methods that need to ensure that the index is available for
     * use.
     * 
     * @see #close()
     * @see #isOpen()
     * @see #getRoot()
     */
    public void reopen();
    
    /**
     * The contract for {@link #close()} is to reduce the resource burden of the
     * index while not rendering the index inoperative. An index that has been
     * {@link #close() closed} MAY be {@link #reopen() reopened} at any time
     * (conditional on the continued availability of the backing store). Such an
     * index reference remains valid after a {@link #close()}. A closed index is
     * transparently {@link #reopen() reopened} by any access to the index data
     * (scanning the index, probing the index, etc).
     * <p>
     * Note: A {@link #close()} on a dirty index MUST discard writes rather than
     * flushing them to the store and MUST NOT update its {@link Checkpoint}
     * record - ({@link #close()} is used to discard indices with partial writes
     * when an {@link AbstractTask} fails). If you are seeking to
     * {@link #close()} a mutable index view that state can be recovered by
     * {@link #reopen()} then you MUST write a new {@link Checkpoint} record
     * before closing the index.
     */
    public void close();
    
    /**
     * An "open" index has may have some buffered data. A closed index will have
     * to re-read any backing data from the backing store.
     * 
     * @return If the index is "open".
     * 
     * @see #close()
     * @see #reopen()
     */
    public boolean isOpen();

    /**
    * Reports statistics for the index.
    * 
    * @param recursive
    *           When <code>true</code>, also collects statistics on the pages
    *           (nodes and leaves) using a low-level approach.
    * @param visitLeaves
    *           When <code>true</code> and <code>recursive:=true</code> then the
    *           leaves of the index are also visited.
    * 
    * @return Some interesting statistics about the index (and optionally the
    *         pages in that index) which the caller can print out.
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1050" > pre-heat the journal
    *      on startup </a>
    */
   BaseIndexStats dumpPages(boolean recursive, final boolean visitLeaves);

}
