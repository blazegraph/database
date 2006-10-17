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
 * Created on Oct 16, 2006
 */

package com.bigdata.journal;

/**
 * <p>
 * Interface for operations on the index mapping int32 within segment persistent
 * identifiers onto slots in the journal. There are always logical levels to the
 * object index. The first is the object index for the journal outside of any
 * transactional scope. There is actually one such index per committed state on
 * the journal. The second is the per-transaction object index.
 * </p>
 * <p>
 * BEGIN obtains a read-only view onto the then-current (BEGIN time) object
 * index for the committed state that will be read by that transaction. Changes
 * to the object index are then are made within the per-transaction object index
 * as new data versions are written or deleted. During PREPARE, the object index
 * is merged with the then-current (PREPARE time, e.g., last committed) object
 * index. A COMMIT makes the merged object index accessible to new transactions.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IObjectIndex {

    /**
     * Update the object index to map the id onto the slot.
     * 
     * @param id
     *            The int32 within-segment persistent identifier.
     * @param slot
     *            The first slot on which the current version of the identified
     *            data is written within this transaction scope.
     */
    public void mapIdToSlot( int id, int slot );
    
    /**
     * Return the first slot on which the current version of the data is stored.
     * If the current version of the object for the transaction is stored on the
     * journal, then this returns the first slot on which that object is
     * written. Otherwise {@link NOTFOUND} is returned to indicate that the
     * current version is stored on the database. An exception is thrown if the
     * persistent identifier has been deleted within the scope visible to the
     * transaction.
     * 
     * @param id
     *            The int32 within-segment persistent identifier.
     * 
     * @return The first slot on which the data version is stored or
     *         {@link #NOTFOUND} if the identifier is not mapped
     * 
     * @exception DataDeletedException
     *                This exception is thrown if the object is logically
     *                deleted on the journal within the scope visible to the
     *                transaction. The caller MUST NOT resolve the persistent
     *                identifier against the database since the current version
     *                is deleted.
     * 
     * @see #NOTFOUND
     */
    public int getFirstSlot( int id );

    /**
     * Removes and returns the first slot on which a version of the deleted data
     * version was last written.
     * 
     * @param id
     *            The int32 within-segment persistent identifier.
     * 
     * @return The first slot.
     * 
     * @exception IllegalArgumentException
     *                if the persistent identifier is bad.
     * @exception IllegalStateException
     *                if <i>id</id> does not identify deleted data.
     */
    public int removeDeleted( int id );
    
    /**
     * Remove the entry in the object index, making the data no longer visible
     * in this transaction scope.
     * 
     * @todo The real contract for this method is a little more complex. It has
     *       to remove the data from visibility within the transaction, but it
     *       also needs to record that the data was deleted so that we can
     *       correctly respond to subsequent reads with a {@link #NOTFOUND} and
     *       properly handle index merges during commit processing.
     * 
     * @param id
     *            The int32 within-segment persistent identifier.
     *            
     * @exception IllegalArgumentException
     *                if the transaction identifier is bad.
     * @exception IllegalArgumentException
     *                if the persistent identifier is bad.
     * 
     */
    public void delete( int id );
    
    /**
     * Indicates that the current data version for the persistent identifier was
     * not found in the journal's object index. An application should test the
     * database when this is returned since the current version MAY exist on the
     * database.
     * 
     * @todo The value of this constant was changed from -1 to
     *       {@link Integer#MIN_VALUE} to remove the possibility of confusing a
     *       deleted slot with a "not found" return.  However, this has implications
     *       for where the fencepost lies for the maximum #of addressable slots in a
     *       journal.  If we continue to use negative values to mark deleted entries
     *       then update those fencepost tests (which need review anyway).
     */
    public static final int NOTFOUND = Integer.MIN_VALUE;

}
