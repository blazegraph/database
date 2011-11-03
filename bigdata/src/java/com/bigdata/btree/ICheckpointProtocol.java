package com.bigdata.btree;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.Name2Addr;
import com.bigdata.journal.Name2Addr.Entry;

/**
 * Interface in support of the {@link Checkpoint} record protocol.
 * 
 * @author thompsonbry@users.sourceforge.net
 */
public interface ICheckpointProtocol extends ICommitter {

	/**
	 * The value of the record version number that will be assigned to the next
	 * node or leaf written onto the backing store. This number is incremented
	 * each time a node or leaf is written onto the backing store. The initial
	 * value is ZERO (0). The first value assigned to a node or leaf will be
	 * ZERO (0).
	 */
	public long getRecordVersion();

	/**
	 * Returns the most recent {@link Checkpoint} record.
	 * 
	 * @return The most recent {@link Checkpoint} record and never
	 *         <code>null</code>.
	 */
    public Checkpoint getCheckpoint();
    
	/**
	 * The address at which the most recent {@link IndexMetadata} record was
	 * written.
	 */
    public long getMetadataAddr();
    
    /**
     * The address of the last written root node or leaf -or- 0L if there is
     * no root and one should be created on demand.
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
    
}
