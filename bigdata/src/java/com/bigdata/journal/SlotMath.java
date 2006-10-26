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
 * Created on Oct 8, 2006
 */

package com.bigdata.journal;

/**
 * Helper class for slot-based operations. A slot is the minimum unit of
 * allocation for the journal. Each slot has a header giving the int32 address
 * of the next slot for the same object or zero (0) if the remainder of the
 * object fits entirely in the current slot. Slots are intended for use within
 * an append oriented data structure. The #of slots required to write an object
 * can be determined before the object is written. Those slots are allocated
 * using the allocation index and then written at once. If insufficient slots
 * are available then slots must be released on the journal.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 *
 * @todo Refactor into journal or {@link AbstractBufferStrategy}.
 * 
 * @see TestSlotMath
 */

public class SlotMath {

//    /**
//     * The size of the per-slot header.
//     * 
//     * @see #headerSize
//     * 
//     * @deprecated The use of slot headers is being phased out.
//     */
//    static final int HEADER_SIZE = 0; // Bytes.SIZEOF_INT + Bytes.SIZEOF_INT;

    /**
     * The slot size in bytes.
     */
    final int slotSize;

//    /**
//     * The size of the per-slot header. The header contains two int32 fields.
//     * These are:
//     * <dl>
//     * <dt>nextSlot or -1</dt>
//     * <dd> The int32 identifier of the next slot in the allocation or negative
//     * one (-1) iff this is the last slot in the chain for some object. The
//     * slots allocated for a given object tend to be close together and are
//     * always monotonically increasing unless the journal wraps around.</dd>
//     * <dt>priorSlot or -size</dt>
//     * <dd>The int32 identifier of the prior slot in the allocation or a
//     * negative number whose unsigned value is the #of bytes in the allocation
//     * iff this is the first slot in the chain for some object. The use of an
//     * size field simplifies reading data from a slot chain since the
//     * destination buffer can be allocated when the first slot is read. The use
//     * of an unsigned value limits the data size to 2^31. However, the journal
//     * is designed to buffer small objects efficiently. Large objects MUST be
//     * written directly on the database or into a filesystem.</dd>
//     * </dl>
//     * The use of a double-linked chain of slots makes it possible to examine
//     * the store on a slot-by-slot basis, chaining backwards and fowards to
//     * discover whether a slot is part of a consistent object. Slots that have
//     * been reused will NOT form consistent chains. Note, however, that it is
//     * NOT possible to identify whether the data in a slot chain is a current
//     * version, what the persistent identifier is for that data, nor in which
//     * transaction the data were written and whether or not that transaction
//     * committed. That information is carried in the object index and the
//     * allocation nodes.
//     * 
//     * @deprecated The use of slot headers is being phased out.
//     */
//    final int headerSize = HEADER_SIZE;
//    
//    /**
//     * The #of bytes per slot that are available for application data.
//     * 
//     * @todo This will be identical to slotSize once the use of slot headers is
//     *       phased out. Those two fields should therefore be collapsed.
//     */
//    final int dataSize;
    
    /**
     * @param slotSize
     *            The slot size in bytes. Slots should be 48-256 bytes for
     *            applications with fine grained objects.
     */
    public SlotMath(int slotSize) {
        
        // This is a heuristic minimum.
        if( slotSize < 16 ) throw new IllegalArgumentException();
        
        this.slotSize = slotSize;
        
//        this.dataSize = slotSize - headerSize;
        
    }

    /**
     * Return the #of slots required to hold the data.
     * 
     * @param nbytes The #of bytes to be written.
     * 
     * @return The #of slots required.
     */
    
    public int getSlotCount(int nbytes) {
    
        if( nbytes <= 0 ) throw new IllegalArgumentException();

//        int nslots = nbytes / dataSize;
        int nslots = nbytes / slotSize;
        
        if( nslots * slotSize < nbytes ) {
            
            //if( nslots * dataSize < nbytes ) {
            
            nslots++;
            
        }
        
        return nslots;
 
    }

    /**
     * Converts a continguous slot allocation to a long integer.
     * 
     * @param nbytes
     *            The #of bytes in the allocation.
     * @param firstSlot
     *            The first slot in the allocation.
     * 
     * @return The long integer.
     * 
     * @exception IllegalArgumentException
     *                if the slot allocation is not contiguous.
     * 
     * @see #toLong(int, int)
     * @see #getByteCount(long)
     * @see #getFirstSlot(long)
     */
    public static long toLong(ISlotAllocation slots) {
        
        if( ! slots.isContiguous() ) throw new IllegalArgumentException();
       
        return toLong( slots.getByteCount(), slots.firstSlot() );
        
    }
    
    /**
     * <p>
     * Converts a (presumed) continguous slot allocation to a long integer.
     * </p>
     * <p>
     * Note: This is package private since it does not verify that the
     * allocation is contiguous. This method should only be used within
     * {@link ISlotAllocation#toLong()} implementations that have already
     * validated that the slots are contiguous.
     * </p>
     * 
     * @param nbytes
     *            The #of bytes in the allocation.
     * @param firstSlot
     *            The first slot in the allocation.
     * 
     * @return The long integer.
     * 
     * @see #toLong(ISlotAllocation)
     * @see #getByteCount(long)
     * @see #getFirstSlot(long)
     */
    static long toLong(int nbytes,int firstSlot) {
        
        return ((long) firstSlot) << 32 | nbytes ;
        
    }
    
    /**
     * Extracts the byte count from a long integer formed by
     * {@link #toLong(int, int)}.
     * 
     * @param longValue
     *            The long integer.
     * @return The byte count in the corresponding slot allocation.
     */
    public static int getByteCount(long longValue) {

        return (int) (NBYTES_MASK & longValue);

    }

    /**
     * Extracts the first slot index from a long integer formed by
     * {@link #toLong(int, int)}. Since the slots are guarenteed to be
     * contiguous
     * 
     * @param longValue
     *            The long integer.
     * @return The first slot index in the corresponding slot allocation.
     */
    public static int getFirstSlot(long longValue) {

        return (int) ((FIRST_SLOT_MASK & longValue) >>> 32);

    }

    private static final transient long NBYTES_MASK     = 0x00000000ffffffffL;
    private static final transient long FIRST_SLOT_MASK = 0xffffffff00000000L;

    /**
     * Convert a long integer into an {@link ISlotAllocation}.
     * 
     * @param longValue A value formed by {@link #toLong(ISlotAllocation)}
     */
    public ISlotAllocation toSlots(long longValue) {

        int nbytes = SlotMath.getByteCount(longValue);

        int nslots = getSlotCount(nbytes);

        int firstSlot = SlotMath.getFirstSlot(longValue);

        return new ContiguousSlotAllocation(nbytes, nslots, firstSlot);

    }

}
