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
 * Created on Feb 3, 2007
 */

package com.bigdata.journal;

import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.IRawStore;

/**
 * Interface for low-level operations on a store supporting an atomic commit.
 * Persistent implementations of this interface are restart-safe.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IAtomicStore extends IRawStore {

    /**
     * Abandon the current write set (immediate, synchronous).
     */
    public void abort();
    
    /**
     * Atomic commit (immediate, synchronous).
     * 
     * @return The timestamp assigned to the {@link ICommitRecord} -or- 0L if
     *         there were no data to commit.
     * 
     * @exception IllegalStateException
     *                if the store is not open.
     * @exception IllegalStateException
     *                if the store is not writable.
     */
    public long commit();

    /**
     * Invoked when a journal is first created, re-opened, or when the
     * committers have been {@link #discardCommitters() discarded}.
     */
    public void setupCommitters();

    /**
     * This method is invoked whenever the store must discard any hard
     * references that it may be holding to objects registered as
     * {@link ICommitter}s.
     */
    public void discardCommitters();
    
    /**
     * Set a persistence capable data structure for callback during the commit
     * protocol.
     * <p>
     * Note: the committers must be reset after restart or whenever 
     * 
     * @param index
     *            The slot in the root block where the {@link Addr address} of
     *            the {@link ICommitter} will be recorded.
     * 
     * @param committer
     *            The commiter.
     */
    public void setCommitter(int index, ICommitter committer);

    /**
     * The last address stored in the specified root slot as of the last
     * committed state of the store.
     * 
     * @param index
     *            The index of the root {@link Addr address}.
     * 
     * @return The {@link Addr address} stored at that index.
     * 
     * @exception IndexOutOfBoundsException
     *                if the index is negative or too large.
     */
    public long getRootAddr(int index);

    /**
     * Return a read-only view of the current root block.
     * 
     * @return The current root block.
     */
    public IRootBlockView getRootBlockView();

    /**
     * Return the {@link ICommitRecord} for the most recent committed state
     * whose commit timestamp is less than or equal to <i>timestamp</i>. This
     * is used by a {@link Tx transaction} to locate the committed state that is
     * the basis for its operations.
     * 
     * @param commitTime
     *            Typically, the commit time assigned to a transaction.
     * 
     * @return The {@link ICommitRecord} for the most recent committed state
     *         whose commit timestamp is less than or equal to <i>timestamp</i>
     *         -or- <code>null</code> iff there are no {@link ICommitRecord}s
     *         that satisify the probe.
     */
    public ICommitRecord getCommitRecord(long commitTime);

}
