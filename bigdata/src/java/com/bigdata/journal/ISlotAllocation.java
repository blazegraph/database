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
 * Created on Oct 22, 2006
 */

package com.bigdata.journal;


/**
 * <p>
 * A slot allocation is initially empty. New slots may be
 * {@link #add(int) added} until the allocation is {@link #closed()}. Once
 * closed, the allocation is read-only. The operations {@link #add(int)} and
 * {@link #close()} are <em>optional</em>.
 * </p>
 * <p>
 * Slot allocations are typically built up as a side effect when a data version
 * is written onto the journal. An efficient persistence capable representation
 * of the slot allocation is stored as part of the {@link IObjectIndex} entry
 * for a persistent identifier. This information may be used to efficiently read
 * slots from the disk. It is also used to efficiently deallocate slots for data
 * versions that are no longer accessible to any active transaction.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISlotAllocation {

    /**
     * Append a slot to this allocation (optional operation).
     * 
     * @param slot
     *            The slot.
     * 
     * @exception IllegalArgumentException
     *                if the slot is negative.
     * 
     * @exception IllegalStateException
     *                if the slot allocation is closed (read-only).
     */
    public void add(int slot);

    /**
     * Close a slot allocation (makes it read-only).
     * 
     * @exception IllegalStateException
     *                if the allocation is already closed.
     */
    public void close();
    
    /**
     * True iff the allocation is closed to further writes.
     * 
     * @return True iff the allocation is closed.
     */
    public boolean isClosed();
    
    /**
     * The #of bytes in the allocation.
     * 
     * @return The #of bytes (positive integer).
     */
    public int getByteCount();
    
    /**
     * The capacity of the allocation (optional operation).
     * 
     * @exception UnsupportedOperationException
     *                If the operation is not supported.
     */
    public int capacity();
    
    /**
     * The #of slots currenting written into the allocation.
     * 
     * @return The #of allocated slots.
     */
    public int getSlotCount();

    /**
     * <p>
     * The first slot in the allocation. Calling this method resets a
     * per-instance cursor that visits in this allocation.
     * </p>
     * <p>
     * The visitation pattern for {@link ISlotAllocation} is NOT thread-safe.
     * </p>
     * 
     * @return The first slot in the allocation.
     * 
     * @exception IllegalStateException
     *                if there are no slots in the allocation.
     */
    public int firstSlot();

    /**
     * <p>
     * Visit the next slot in the allocation.
     * </p>
     * <p>
     * The visitation pattern for {@link ISlotAllocation} is NOT thread-safe.
     * </p>
     * 
     * @return The next slot in the allocation or <code>-1</code> IFF the last
     *         slot was just read using {@link #nextSlot()}.
     * 
     * @exception IllegalStateException
     *                if the cursor has already indicated that it is exhausted
     *                by returning <code>-1</code> from this method.
     * 
     * @exception IllegalStateException
     *                if the cursor has not been initialized by calling
     *                {@link #firstSlot()}
     */
    public int nextSlot(); 

    /**
     * Return true iff the allocation is contiguous.
     * 
     * @exception IllegalStateException
     *                if there is nothing allocated.
     */
    public boolean isContiguous();
    
    /**
     * A slot allocation created from contiguous slots may be represented as a
     * long integer. The int32 within segment persistent identifier is placed
     * into the high word and the low word is the size of the allocation. Since
     * the slots are contiguous, the allocation may be recovered from this data.
     * 
     * @return The allocation expressed as a long integer.
     * 
     * @exception IllegalStateException
     *                if the slots in the allocation are not contiguous.
     */
    public long toLong();
    
}
