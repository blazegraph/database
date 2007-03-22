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
import java.util.UUID;

import com.bigdata.objndx.BTreeMetadata;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.Bytes;
import com.bigdata.util.TimestampFactory;

/**
 * A view onto a root block of the {@link Journal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo add checksum field to the root blocks and maintain it. we don't really
 *       need a magic for the root blocks if we use a checksum. or maybe it is
 *       [magic,checksum,[data]] with the timestamps inside of the checksumed
 *       data region. the {@link #OFFSET_UNUSED1} field might be put to this
 *       purpose.
 */
public class RootBlockView implements IRootBlockView {

    static final transient short SIZEOF_TIMESTAMP  = Bytes.SIZEOF_LONG;
    static final transient short SIZEOF_MAGIC      = Bytes.SIZEOF_INT;
    static final transient short SIZEOF_VERSION    = Bytes.SIZEOF_INT;
//    static final transient short SIZEOF_SEGMENT_ID = Bytes.SIZEOF_INT;
    static final transient short SIZEOF_ADDR       = Bytes.SIZEOF_LONG;
    static final transient short SIZEOF_COUNTER    = Bytes.SIZEOF_LONG;
    static final transient short SIZEOF_OFFSET     = Bytes.SIZEOF_INT;
    // Note: a chunk of reserved bytes.
    static final transient short SIZEOF_UNUSED     = 256-Bytes.SIZEOF_UUID;
    
//  static final transient short OFFSET_CHECKSUM   =  
    static final transient short OFFSET_TIMESTAMP0 = 0;
    static final transient short OFFSET_MAGIC      = OFFSET_TIMESTAMP0  + SIZEOF_TIMESTAMP;
    static final transient short OFFSET_VERSION    = OFFSET_MAGIC       + SIZEOF_MAGIC;
    static final transient short OFFSET_UNUSED1    = OFFSET_VERSION     + SIZEOF_VERSION;
    static final transient short OFFSET_NEXT_OFFSET= OFFSET_UNUSED1     + Bytes.SIZEOF_INT;
    static final transient short OFFSET_FIRST_CMIT = OFFSET_NEXT_OFFSET + SIZEOF_OFFSET;
    static final transient short OFFSET_LAST_CMIT  = OFFSET_FIRST_CMIT    + SIZEOF_TIMESTAMP;
    static final transient short OFFSET_COMMIT_TS  = OFFSET_LAST_CMIT     + SIZEOF_TIMESTAMP;
    static final transient short OFFSET_COMMIT_CTR = OFFSET_COMMIT_TS   + SIZEOF_TIMESTAMP;
    static final transient short OFFSET_COMMIT_REC = OFFSET_COMMIT_CTR  + SIZEOF_COUNTER;
    static final transient short OFFSET_COMMIT_NDX = OFFSET_COMMIT_REC  + SIZEOF_ADDR;
    static final transient short OFFSET_UNUSED     = OFFSET_COMMIT_NDX  + SIZEOF_ADDR;
    static final transient short OFFSET_UUID       = OFFSET_UNUSED      + SIZEOF_UNUSED;
    static final transient short OFFSET_TIMESTAMP1 = OFFSET_UUID        + Bytes.SIZEOF_UUID;
    static final transient short SIZEOF_ROOT_BLOCK = OFFSET_TIMESTAMP1  + SIZEOF_TIMESTAMP;

    /**
     * Magic value for root blocks.
     */
    final int MAGIC = 0x65fe21bc;

    /**
     * This is the only version defined so far.
     */
    final int VERSION0 = 0x0;
    
    private final ByteBuffer buf;
    
    private final boolean rootBlock0;
    
    public boolean isRootBlock0() {
        
        return rootBlock0;
        
    }
    
    /**
     * Create a new read-only root block image with a unique timestamp. The
     * other fields are populated from the supplied parameters.
     * 
     * @param nextOffset
     *            The next offset at which a record will be written on the
     *            store.
     * @param firstCommitTime
     *            The timestamp of the earliest commit on the store or zero (0L)
     *            iff there have been no commits.
     * @param lastCommitTime
     *            The timestamp of the most recent commit on the store or zero
     *            (0L) iff there have been no commits.
     * @param commitTime
     *            The timestamp assigned to this commit - this is distinct from
     *            the timestamps written as part of the Challis algorithm. The
     *            latter exist solely to detect commit failures, while this
     *            timestamp is assigned by the transaction commit protocol.
     * @param commitCounter
     *            The commit counter. This should be ZERO (0L) for a new
     *            journal. For an existing journal, the value should be
     *            incremented by ONE (1) each time the root block is written (as
     *            part of a commit naturally).
     * @param commitRecordAddr
     *            The {@link Addr address} at which the {@link ICommitRecord}
     *            containing the root addresses was written or 0L if there are
     *            no root addresses (this is true when the store is first
     *            created).
     * @param commitRecordIndexAddr
     *            The {@link Addr address} at which the {@link BTreeMetadata}
     *            for the {@link CommitRecordIndex} was written or 0L if there
     *            are no historical {@link ICommitRecord}s (this is true when
     *            the store is first created).
     * @param uuid
     *            The unique journal identifier.
     */
//    * @param segmentId
//    *            The segment identifier for the journal.
    RootBlockView(boolean rootBlock0, /*int segmentId,*/ int nextOffset,
            long firstCommitTime, long lastCommitTime, long commitTimestamp,
            long commitCounter, long commitRecordAddr,
            long commitRecordIndexAddr, UUID uuid) {

        if (nextOffset < 0)
            throw new IllegalArgumentException("nextOffset is negative.");
        if( firstCommitTime == 0L && lastCommitTime != 0L)
            throw new IllegalArgumentException("first transaction identifier is zero, but last transaction identifier is not.");
        if (firstCommitTime != 0 && lastCommitTime < firstCommitTime)
            throw new IllegalArgumentException("last transaction identifier is less than first transaction identifier.");
        if( lastCommitTime != 0 && commitTimestamp < lastCommitTime) {
            throw new IllegalArgumentException("commit counter must be greater than the start time of the last committed transactions");
        }
        if (commitCounter < 0)
            throw new IllegalArgumentException("commit counter is zero.");
        if (commitCounter == Long.MAX_VALUE )
            throw new IllegalArgumentException("commit counter would overflow.");
        if( commitRecordAddr < 0 ) 
            throw new IllegalArgumentException("Invalid address for the commit record.");
        if( commitRecordIndexAddr < 0 ) 
            throw new IllegalArgumentException("Invalid address for the commit record index.");
        if (commitCounter > 0) {
            if (commitRecordAddr == 0)
                throw new IllegalArgumentException(
                        "The commit record must exist if the commit counter is non-zero");
            if (commitRecordIndexAddr == 0)
                throw new IllegalArgumentException(
                        "The commit record index must exist if the commit counter is non-zero");
        }
        if (commitRecordAddr > 0 && commitRecordIndexAddr == 0) {
            throw new IllegalArgumentException(
                    "The commit record index must exist if there is a commit record.");
        }
        if (commitRecordIndexAddr > 0 && commitRecordAddr == 0) {
            throw new IllegalArgumentException(
                    "The commit record address must exist if there is a commit record index.");
        }
        if(uuid == null) {
            throw new IllegalArgumentException("UUID is null");
        }
        
        buf = ByteBuffer.allocate(SIZEOF_ROOT_BLOCK);
        
        this.rootBlock0 = rootBlock0;
        
        final long rootBlockTimestamp = TimestampFactory.nextNanoTime();

        buf.putLong(rootBlockTimestamp);
        buf.putInt(MAGIC);
        buf.putInt(VERSION0);
        buf.putInt(0); // unused field
        buf.putInt(nextOffset);
        buf.putLong(firstCommitTime);
        buf.putLong(lastCommitTime);
        buf.putLong(commitTimestamp);
        buf.putLong(commitCounter);
        buf.putLong(commitRecordAddr);
        buf.putLong(commitRecordIndexAddr);
        buf.position(buf.position()+SIZEOF_UNUSED); // skip unused region.
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        buf.putLong(rootBlockTimestamp);

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

    public int getVersion() {
        
        return buf.getInt(OFFSET_VERSION);
        
    }

    public int getNextOffset() {
        
        return buf.getInt(OFFSET_NEXT_OFFSET);
        
    }

    public long getFirstCommitTime() {
        
        return buf.getLong(OFFSET_FIRST_CMIT);
        
    }
    
    public long getLastCommitTime() {
        
        return buf.getLong(OFFSET_LAST_CMIT);
        
    }
    
    public long getRootBlockTimestamp() throws RootBlockException {
        
        long timestamp0 = buf.getLong(OFFSET_TIMESTAMP0);
        
        long timestamp1 = buf.getLong(OFFSET_TIMESTAMP1);
        
        if( timestamp0 != timestamp1 ) {
            
            throw new RootBlockException("Timestamps differ: "+timestamp0 +" vs "+ timestamp1);
            
        }
        
        return timestamp0;
        
    }

    public long getCommitTimestamp() {
        
        return buf.getLong(OFFSET_COMMIT_TS);
        
    }
    
    public long getCommitCounter() {
        
        return buf.getLong(OFFSET_COMMIT_CTR);
        
    }
    
    public long getCommitRecordAddr() {
        
        return buf.getLong(OFFSET_COMMIT_REC);
        
    }

    public long getCommitRecordIndexAddr() {
        
        return buf.getLong(OFFSET_COMMIT_NDX);
        
    }

    public void valid() {
        
        final int magic = buf.getInt(OFFSET_MAGIC);
        
        if( magic != MAGIC ) {

            throw new RuntimeException("MAGIC: expected="+MAGIC+", actual="+magic);
            
        }

        final int version = buf.getInt(OFFSET_VERSION);
        
        if( version != VERSION0 ) {
            
            throw new RuntimeException("Unknown version: "+version);
            
        }
        
        // test timestamps.
        getRootBlockTimestamp();
        
    }

    public UUID getUUID() {
        
        return new UUID(buf.getLong(OFFSET_UUID)/* MSB */, buf
                .getLong(OFFSET_UUID + 8)/*LSB*/);
        
    }
    
//    public int getSegmentId() {
//
//        return buf.getInt(OFFSET_SEGMENT_ID);
//        
//    }

    public String toString() {
    
        StringBuilder sb = new StringBuilder();
        
        sb.append("rootBlock");
        
        sb.append("{ rootBlockTimestamp="+getRootBlockTimestamp());
        sb.append(", version="+getVersion());
//        sb.append(", segmentId="+getSegmentId());
        sb.append(", nextOffset="+getNextOffset());
        sb.append(", firstCommitTime="+getFirstCommitTime());
        sb.append(", lastCommitTime="+getLastCommitTime());
        sb.append(", commitTime="+getCommitTimestamp());
        sb.append(", commitCounter="+getCommitCounter());
        sb.append(", commitRecordAddr="+Addr.toString(getCommitRecordAddr()));
        sb.append(", commitRecordIndexAddr="+Addr.toString(getCommitRecordIndexAddr()));
        sb.append(", uuid="+getUUID());
        
        sb.append("}");
        
        return sb.toString();
        
    }
    
}
