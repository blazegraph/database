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
 * Created on Oct 18, 2006
 */

package com.bigdata.journal;

import java.nio.ByteBuffer;

/**
 * A view onto a root block of the {@link Journal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Add metadata field for interesting counters to the root block, e.g.:
 *       the #of non-deleted objects on the journal (so that we know whether or
 *       not it is empty), the depth of the object index, the #of free slots,
 *       etc. Since this is a bit wide open, there may be some evolution in both
 *       this interface and the {@link RootBlockView}. That evolution needs to
 *       get locked down at some point. Verify that we can version the Journal
 *       safely so as to be able to read and write journals that have an older
 *       root block format.
 * 
 * FIXME Add a field for the last transaction identifier to commit on the
 * journal. As long as the globally assigned transaction identifiers are
 * monotonically increasing (and a lot of things depend on this) then we could
 * use this field in place of the commit counter field to determine which root
 * block was more current.
 * 
 * FIXME Add a field for the last transaction committed on the journal whose
 * data was deleted from the journal. As data versions written in a transaction
 * are no longer visible to active (post-PREPAREd) transactions the slots
 * corresponding to those versions are deallocated on the journal and this field
 * is updated. (Each commit record MUST contain a both the transaction identifer
 * for the prior commit record and the slot index on which that commit record
 * was written, forming a singly linked chain of prior commit records. When
 * traversing that chain, this root block field MUST be checked to determine
 * whether the prior commit record still exists. If the transaction identifier
 * for the prior commit record is equal to or less than this root block field
 * then the transaction is GONE from the journal.)
 */
public class RootBlockView implements IRootBlockView {

    static final short SIZEOF_TIMESTAMP  = Bytes.SIZEOF_LONG;
    static final short SIZEOF_MAGIC      = Bytes.SIZEOF_INT;
    static final short SIZEOF_SEGMENT_ID = Bytes.SIZEOF_LONG; // Note: Could be INT.
    static final short SIZEOF_SLOT_SIZE  = Bytes.SIZEOF_INT; // Note: Could be SHORT.
    static final short SIZEOF_SLOT_LIMIT = Bytes.SIZEOF_INT;
    static final short SIZEOF_SLOT_INDEX = Bytes.SIZEOF_INT;
    
    static final short OFFSET_TIMESTAMP0 = 0;
    static final short OFFSET_MAGIC      = OFFSET_TIMESTAMP0 + SIZEOF_TIMESTAMP;
    static final short OFFSET_SEGMENT_ID = OFFSET_MAGIC      + SIZEOF_MAGIC;
    static final short OFFSET_SLOT_SIZE  = OFFSET_SEGMENT_ID + SIZEOF_SEGMENT_ID;
    static final short OFFSET_SLOT_LIMIT = OFFSET_SLOT_SIZE  + SIZEOF_SLOT_SIZE;
    static final short OFFSET_SLOT_CHAIN = OFFSET_SLOT_LIMIT + SIZEOF_SLOT_LIMIT;
    static final short OFFSET_OBJECT_NDX = OFFSET_SLOT_CHAIN + SIZEOF_SLOT_INDEX;
    static final short OFFSET_COMMIT_CTR = OFFSET_OBJECT_NDX + SIZEOF_SLOT_INDEX;
    static final short OFFSET_TIMESTAMP1 = OFFSET_COMMIT_CTR + Bytes.SIZEOF_LONG;
    static final short SIZEOF_ROOT_BLOCK = OFFSET_TIMESTAMP1 + SIZEOF_TIMESTAMP;

    /**
     * Magic value for root blocks.
     */
    final int MAGIC = 0x65fe21bc;
    
    private final ByteBuffer buf;
    
    private final boolean rootBlock0;
    
    public boolean isRootBlock0() {
        
        return rootBlock0;
        
    }
    
    /**
     * Create a new read-only root block image with a unique timestamp. The
     * other fields are populated from the supplied parameters.
     * 
     * @param segmentId
     *            The segment identifier for the journal.
     * @param slotSize
     *            The slot size for the journal.
     * @param slotLimit
     *            The slot limit for the journal (the #of slots).
     * @param slotChain
     *            The slot index of the slot that is the head of the slot
     *            allocation chain. See {@link ISlotAllocationIndex}.
     * @param objectIndex
     *            The slot index of the slot that is the root of the object
     *            index. See {@link IObjectIndex}.
     * @param commitCounter
     *            The commit counter. This should be ZERO (0L) for a new
     *            journal. For an existing journal, the value should be
     *            incremented by ONE (1) each time the root block is written (as
     *            part of a commit naturally).
     */
    RootBlockView(boolean rootBlock0, long segmentId, int slotSize,
            int slotLimit, int slotChain, int objectIndex, long commitCounter) {

        if (slotSize < Journal.MIN_SLOT_SIZE)
            throw new IllegalArgumentException();
        if (slotLimit <= 0)
            throw new IllegalArgumentException();
        if (slotChain < 0 || slotChain >= slotLimit)
            throw new IllegalArgumentException();
        if (objectIndex < 0 || objectIndex >= slotLimit)
            throw new IllegalArgumentException();
        if (commitCounter < 0 || commitCounter == Long.MAX_VALUE )
            throw new IllegalArgumentException();
        
        buf = ByteBuffer.allocate(SIZEOF_ROOT_BLOCK);
        
        this.rootBlock0 = rootBlock0;
        
        final long timestamp = TimestampFactory.nextNanoTime();

        buf.putLong(timestamp);
        buf.putInt(MAGIC);
        buf.putLong(segmentId);
        buf.putInt(slotSize);
        buf.putInt(slotLimit);
        buf.putInt(slotChain);
        buf.putInt(objectIndex);
        buf.putLong(commitCounter);
        buf.putLong(timestamp);

        assert buf.limit() == SIZEOF_ROOT_BLOCK;

        buf.position(0);
        
    }

    /**
     * A read-only buffer whose contents are the root block.
     */
    public ByteBuffer asReadOnlyBuffer() {

        return buf;
        
    }
    
    /**
     * Create a new read-only view of the region of the supplied buffer from its
     * current position to its current limit.
     * 
     * 
     * @param rootBlock0
     *            There are two root blocks and they are written in an
     *            alternating order. For the sake of distinction, the first one
     *            is referred to as "rootBlock0" while the 2nd one is referred
     *            to as "rootBlock1". This parameter allows the caller to store
     *            a transient field on the view that indicates which root block
     *            it represents.
     * @param buf
     *            The buffer. If the buffer is modified in this region, those
     *            changes will be immediately reflected in the methods on the
     *            created {@link RootBlockView} object.
     * 
     * @exception IllegalArgumentException
     *                if the buffer is null or if the #of bytes remaining in the
     *                buffer is not exactly {@link #SIZEOF_ROOT_BLOCK}.
     * @exception RootBlockException
     *                if the root block is not valid (bad magic, timestamps do
     *                not agree, etc).
     */
    RootBlockView(boolean rootBlock0, ByteBuffer buf) throws RootBlockException {
        
        if( buf == null ) throw new IllegalArgumentException();
        
        if( buf.remaining() != SIZEOF_ROOT_BLOCK ) {
            
            throw new IllegalArgumentException("Expecting " + SIZEOF_ROOT_BLOCK
                    + " remaining, acutal=" + buf.remaining());
            
        }
        
        this.buf = buf.slice().asReadOnlyBuffer();
        
        this.rootBlock0 = rootBlock0;
        
        valid();
        
    }
    
    public int getObjectIndexRoot() {
        return buf.getInt(OFFSET_OBJECT_NDX);
    }

    public int getSlotIndexChainHead() {
        return buf.getInt(OFFSET_SLOT_CHAIN);
    }

    public int getSlotLimit() {
        return buf.getInt(OFFSET_SLOT_LIMIT);
    }

    public int getSlotSize() {
        return buf.getInt(OFFSET_SLOT_SIZE);
    }

    public long getTimestamp() throws RootBlockException {
        
        long timestamp0 = buf.getLong(OFFSET_TIMESTAMP0);
        
        long timestamp1 = buf.getLong(OFFSET_TIMESTAMP1);
        
        if( timestamp0 != timestamp1 ) {
            
            throw new RootBlockException("Timestamps differ: "+timestamp0 +" vs "+ timestamp1);
            
        }
        
        return timestamp0;
        
    }

    public long getCommitCounter() {
        
        return buf.getLong(OFFSET_COMMIT_CTR);
        
    }
    
    public void valid() {
        
        int magic = buf.getInt(OFFSET_MAGIC);
        
        if( magic != MAGIC ) {

            throw new RuntimeException("MAGIC: expected="+MAGIC+", actual="+magic);
            
        }

        // test timestamps.
        getTimestamp();
        
    }

    public long getSegmentId() {

        return buf.getLong(OFFSET_SEGMENT_ID);
        
    }

}
