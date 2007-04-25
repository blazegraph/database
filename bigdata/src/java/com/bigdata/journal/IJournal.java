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
package com.bigdata.journal;

import java.util.Properties;

import com.bigdata.btree.IIndex;

/**
 * <p>
 * An append-only persistence capable data structure supporting atomic commit,
 * scalable named indices, and transactions. Writes are logically appended to
 * the journal to minimize disk head movement.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IJournal extends IMROW, IAtomicStore, IIndexManager {

    /**
     * A copy of the properties used to initialize this journal.
     * 
     * @todo consider making the properties restart safe so that they can be
     *       read from the journal. This will let some properties be specified
     *       on initialization while letting others default or be overriden on
     *       restart. This is trivially accomplished by dedicating a root slot
     *       to a Properties object, or a flattened Properties object serialized
     *       as key-value pairs, in which case the data could just be loaded
     *       into a btree and the btree api could be used to change the
     *       persistent properties as necessary.
     */
    public Properties getProperties();
    
    /**
     * An overflow condition arises when the journal is within some declared
     * percentage of its maximum capacity during a {@link #commit()}. If this
     * event is not handled then the journal will automatically extent itself
     * until it either runs out of address space (int32) or other resources.
     * 
     * @return true iff the overflow event was handled (e.g., if a new journal
     *         was created to absorb subsequent writes). if a new journal is NOT
     *         opened then this method should return false.
     */
    public boolean overflow();
    
    /**
     * Shutdown the journal politely. Scheduled operations will run to
     * completion, but no new operations will be scheduled.
     */
    public void shutdown();

    /**
     * Return the named index which MAY may be invalidated by a
     * {@link IAtomicStore#commit()}.
     */
    public IIndex getIndex(String name);

    /**
     * Return the named index as isolated by the transaction having the
     * specified transaction start time.
     * 
     * @param name
     *            The index name.
     * @param startTime
     *            The transaction start time, which serves as the unique
     *            identifier for the transaction.
     * 
     * @return The isolated index.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>
     * 
     * @exception IllegalStateException
     *                if there is no active transaction with that timestamp.
     */
    public IIndex getIndex(String name, long startTime);

}
