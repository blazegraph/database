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
     * Abandon the current write set.
     */
    public void abort();
    
    /**
     * Request an atomic commit.
     * 
     * @exception IllegalStateException
     *                if the store is not open.
     * @exception IllegalStateException
     *                if the store is not writable.
     */
    public void commit();

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
     * @param rootSlot
     *            The slot in the root block where the {@link Addr address} of
     *            the {@link ICommitter} will be recorded.
     * 
     * @param committer
     *            The commiter.
     */
    public void setCommitter(int rootSlot, ICommitter committer);

    /**
     * The last address stored in the specified root slot as of the last
     * committed state of the store.
     * 
     * @param rootSlot
     *            The root slot identifier.
     * 
     * @return The {@link Addr address} written in that slot.
     */
    public long getAddr(int rootSlot);

}
