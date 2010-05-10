/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
/*
 * Created on Oct 18, 2006
 */

package com.bigdata.journal;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.ha.Quorum;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.rwstore.RWStore;
import com.bigdata.util.ChecksumUtility;

/**
 * A view onto a root block of the {@link Journal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RootBlockView implements IRootBlockView {

    /**
     * Logger.
     */
    public static final Logger log = Logger.getLogger(RootBlockView.class);

    static final transient short SIZEOF_TIMESTAMP  = Bytes.SIZEOF_LONG;
    static final transient short SIZEOF_MAGIC      = Bytes.SIZEOF_INT;
    static final transient short SIZEOF_VERSION    = Bytes.SIZEOF_INT;
    static final transient short SIZEOF_ADDR       = Bytes.SIZEOF_LONG;
    static final transient short SIZEOF_COUNTER    = Bytes.SIZEOF_LONG;
    static final transient short SIZEOF_OFFSET     = Bytes.SIZEOF_LONG;
    static final transient short SIZEOF_CHECKSUM   = Bytes.SIZEOF_INT;
    static final transient short SIZEOF_QTOKEN     = Bytes.SIZEOF_LONG;
    /**
     * This is a chunk of reserved bytes from which new fields in the root block
     * are allocated from time to time.
     */
    static final transient short SIZEOF_UNUSED = 256 - (//
            SIZEOF_QTOKEN     + // quorum token since version 0x02.
            SIZEOF_ADDR       + // metaBitsAddr since version 0x01.
            SIZEOF_ADDR       + // metaStartAddr since version 0x01.
            Bytes.SIZEOF_BYTE + // storeType since version 0x01.  assume zero before that.
            Bytes.SIZEOF_UUID + // uuid
            Bytes.SIZEOF_BYTE + // offsetBits
            SIZEOF_TIMESTAMP  + // createTime
            SIZEOF_TIMESTAMP    // closeTime
            );
    
    static final transient short OFFSET_CHALLIS0   = 0;
    static final transient short OFFSET_MAGIC      = OFFSET_CHALLIS0  + SIZEOF_TIMESTAMP;
    static final transient short OFFSET_VERSION    = OFFSET_MAGIC       + SIZEOF_MAGIC;
    static final transient short OFFSET_OFFSET_BITS= OFFSET_VERSION     + SIZEOF_VERSION;
    static final transient short OFFSET_NEXT_OFFSET= OFFSET_OFFSET_BITS + Bytes.SIZEOF_BYTE;
    static final transient short OFFSET_LOCAL_TIME = OFFSET_NEXT_OFFSET + SIZEOF_OFFSET;
    static final transient short OFFSET_FIRST_CMIT = OFFSET_LOCAL_TIME  + SIZEOF_TIMESTAMP;
    static final transient short OFFSET_LAST_CMIT  = OFFSET_FIRST_CMIT  + SIZEOF_TIMESTAMP;
    static final transient short OFFSET_COMMIT_CTR = OFFSET_LAST_CMIT   + SIZEOF_TIMESTAMP;
    static final transient short OFFSET_COMMIT_REC = OFFSET_COMMIT_CTR  + SIZEOF_COUNTER;
    static final transient short OFFSET_COMMIT_NDX = OFFSET_COMMIT_REC  + SIZEOF_ADDR;
    static final transient short OFFSET_CREATE_TIME= OFFSET_COMMIT_NDX  + SIZEOF_ADDR;
    static final transient short OFFSET_CLOSE_TIME = OFFSET_CREATE_TIME + SIZEOF_TIMESTAMP;
    static final transient short OFFSET_UNUSED     = OFFSET_CLOSE_TIME  + SIZEOF_TIMESTAMP;
//    static final transient short OFFSET_UNUSED     = OFFSET_COMMIT_NDX  + SIZEOF_ADDR;
    static final transient short OFFSET_QTOKEN     = OFFSET_UNUSED      + SIZEOF_UNUSED;
    static final transient short OFFSET_META_BITS  = OFFSET_QTOKEN      + SIZEOF_QTOKEN;
    static final transient short OFFSET_META_START = OFFSET_META_BITS   + SIZEOF_ADDR;
    static final transient short OFFSET_STORETYPE  = OFFSET_META_START  + SIZEOF_ADDR;
    static final transient short OFFSET_UUID       = OFFSET_STORETYPE   + Bytes.SIZEOF_BYTE;
    static final transient short OFFSET_CHALLIS1   = OFFSET_UUID        + Bytes.SIZEOF_UUID;
    static final transient short OFFSET_CHECKSUM   = OFFSET_CHALLIS1    + SIZEOF_TIMESTAMP;  
    static final transient short SIZEOF_ROOT_BLOCK = OFFSET_CHECKSUM    + SIZEOF_CHECKSUM;
    // Note: SIZEOF_ROOT_BLOCK := 340 Bytes.  This is an invariant.

    /**
     * Magic value for root blocks.
     */
    static final int MAGIC = 0x65fe21bc;

    /**
     * Original version.
     */
    static final int VERSION0 = 0x0;

    /**
     * This version supports the {@link RWStore} as well as the log-structured
     * journal store. The {@link RWStore} includes meta-allocation blocks,
     * allocation blocks, and delete blocks (chains of addresses which can be
     * freed in the allocation blocks when the corresponding commit record is
     * discarded).
     * <p>
     * The new fields for this version include:
     * <dl>
     * <dt>storeType</dt>
     * <dd>A byte value which specifies whether the backing store is a journal
     * (log-structured store or WORM) or a read-write store. Only two values are
     * defined at present. See {@link StoreTypeEnum}. The default value of this
     * field before {@link #VERSION1} is {@link StoreTypeEnum#WORM}.</dd>
     * <dt>metaBitsAddr</dt>
     * <dd>Where we will read the metadata bits from. When we start the store up
     * we need to retrieve the metabits from this address. This is a byte offset
     * into the file and is stored as a long integer. Normal addresses are
     * calculated with reference to the allocation blocks. The value for a WORM
     * store and the default value before {@link #VERSION1} are both ZERO (0).</dd>
     * <dt>metaStartAddr</dt>
     * <dd>The start of the area of the file where the allocation blocks are
     * allocated. This is also a byte offset into the file and is stored as a
     * 64-bit integer. It is called metaStartAddr because that is the offset
     * that is used with the metaBitsAddr to determine how to find the
     * allocation blocks. The value for a WORM store and the default value
     * before {@link #VERSION1} are both ZERO (0).</dd>
     * </dl>
     * In addition, the semantics of the following fields are different for the
     * {@link RWStore}
     * <dl>
     * <dt>nextOffset</dt>
     * <dd>For the {@link RWStore}, this the next offset in file from which a
     * large chunk would be allocated in order to obtain space for additional
     * allocation blocks. This field does not change when user level allocations
     * are made.</dd>
     * <dt>offsetBits</dt>
     * <dd>This field is ignored by the {@link RWStore}.</dd>
     * </dl>
     * The head of the delete block chain is stored in the {@link ICommitRecord}
     * so we know the address of the start of each delete block chain for each
     * commit point on the store, which is why it is not stored in the root
     * blocks.
     * 
     * @todo allocation block sizes to allocate?
     */
    static final int VERSION1 = 0x1;

    /**
     * This version supports the HA journal.
     * <p>
     * The new fields for this version include:
     * <dl>
     * <dt>quorumToken</dt>
     * <dd>The {@link Quorum} token associated with the commit point. The
     * default value before {@link #VERSION2} is {@link Quorum#NO_QUORUM}.</dd>
     * </dl>
     */
    static final int VERSION2 = 0x2;

    /**
     * The current version for new root blocks. While different kinds of backing
     * store (e.g., the RW and WORM as of this time) may have some fields which
     * are not used by the other kinds of backing stores, they ALL share the
     * same root block versioning system. Further, all evolutions in the root
     * block versioning MUST be backwards compatible (earlier versions must be
     * readable). Finally, new root block images MUST be formed using the
     * {@link #currentVersion}.
     */
    static final int currentVersion = VERSION2;
    
    /**
     * The buffer holding the backing data.
     */
    private final ByteBuffer buf;

    /**
     * True iff this is root block ZERO (0) as declared by the constructor (this
     * value is not persistent in the root block itself).
     */
    private final boolean rootBlock0;

    /**
     * When non-null, this is used to compute and validate the checksum of the
     * root block. Since the {@link ChecksumUtility} is NOT thread-safe, the
     * {@link AbstractJournal} holds a reference to an instance of this object
     * that is reused for all root blocks read or written by that class.
     */
    private final ChecksumUtility checker;
    
    /**
     * Used for assertion testing in the constructor and by {@link #toString()}.
     */
    private final IAddressManager am;
    
    public boolean isRootBlock0() {
        
        return rootBlock0;
        
    }
    
    /**
     * Note: all addresses are potentially legal with the WormAddressManager so
     * we actually need to have the WormAddressManager to decode the address in
     * order to determine whether or it is legal. The test that we make compares
     * the nextOffset that to be written on the store with the range of bytes
     * covered by the given address. It is an error if the given address would
     * extend beyond the nextOffset to be assigned.
     * 
     * @param am
     * 
     * @param nextOffset
     *            The nextOffset that would be assigned to a record in the
     *            store.
     *            
     * @param addr
     *            An address to be checked for validity (may be
     *            {@link IAddressManager#NULL}).
     */
    static private void assertLegalAddr(final IAddressManager am,
            final long nextOffset, final long addr, final String label) {

        if(addr==0L) return;
        
        // TODO develop protocol to support address checking
        if (am instanceof RWStrategy.RWAddressManager) return;
        
        final long offset = am.getOffset(addr);
        
        final int nbytes = am.getByteCount(addr);
        
        final long nextOffset2 = offset + nbytes;
        
        if (nextOffset2 > nextOffset) { 
         
            throw new IllegalArgumentException(label
                    + " address range ["
                    + am.getOffset(addr) + ":" + am.getOffset(addr)
                    + am.getByteCount(addr)
                    + "] exceeds use extent: nextOffset=" + nextOffset
                    + ", addr=" + am.toString(addr));
            
        }

    }

    /**
     * @deprecated Let's keep just one ctor to create root blocks from state
     *             parameters and one to create root blocks from a
     *             {@link ByteBuffer}.
     */
    RootBlockView(boolean rootBlock0, int offsetBits, long nextOffset,
            long firstCommitTime, long lastCommitTime, long commitCounter,
            long commitRecordAddr, long commitRecordIndexAddr, UUID uuid,
            long quorumToken,
            long createTime, long closeTime, ChecksumUtility checker) {

        this(rootBlock0, offsetBits, nextOffset,
                firstCommitTime, lastCommitTime, commitCounter,
                commitRecordAddr, commitRecordIndexAddr, uuid,
                quorumToken, //
                0L, // metaStartAddr
                0L, // metaBitsAddr
                StoreTypeEnum.WORM, //
                createTime, closeTime, checker);
    
    }

    /**
     * Create a new read-only root block image with a unique timestamp. The
     * other fields are populated from the supplied parameters.
     * 
     * @param offsetBits
     *            The #of bits out of a 64-bit long integer that are used to
     *            encode the byte offset as an unsigned integer. The remaining
     *            bits are used to encode the byte count (aka record length) as
     *            an unsigned integer.
     * @param nextOffset
     *            The next offset at which a record will be written on the
     *            store.
     * @param firstCommitTime
     *            The timestamp of the earliest commit on the store or zero (0L)
     *            iff there have been no commits.
     * @param lastCommitTime
     *            The timestamp of the most recent commit on the store or zero
     *            (0L) iff there have been no commits. This is distinct from the
     *            values written as part of the Challis algorithm. The latter
     *            exist solely to detect commit failures. This timestamp is
     *            assigned by the transaction commit protocol.
     * @param commitCounter
     *            The commit counter. This should be ZERO (0L) for a new
     *            journal. For an existing journal, the value should be
     *            incremented by ONE (1) each time the root block is written (as
     *            part of a commit naturally).
     * @param commitRecordAddr
     *            The address at which the {@link ICommitRecord} containing the
     *            root addresses was written or 0L if there are no root
     *            addresses (this is true when the store is first created).
     * @param commitRecordIndexAddr
     *            The address at which the {@link IndexMetadata} for the
     *            {@link CommitRecordIndex} was written or 0L if there are no
     *            historical {@link ICommitRecord}s (this is true when the store
     *            is first created).
     * @param uuid
     *            The unique journal identifier.
     * @param quorumToken
     *            The current quorum token if this commit point is part of a
     *            {@link Quorum}.
     * @param metaStartAddr
     *            For the {@link StoreTypeEnum#RW} store, the start of the area
     *            of the file where the allocation blocks are allocated. This is
     *            also a byte offset into the file and is stored as a 64-bit
     *            integer. It is called metaStartAddr because that is the offset
     *            that is used with the metaBitsAddr to determine how to find
     *            the allocation blocks. The value for a WORM store is ZERO (0).
     * @param metaBitsAddr
     *            For the {@link StoreTypeEnum#RW} store, where we will read the
     *            metadata bits from. When we start the store up we need to
     *            retrieve the metabits from this address. This is a byte offset
     *            into the file and is stored as a long integer. Normal
     *            addresses are calculated with reference to the allocation
     *            blocks. The value for a WORM store is ZERO (0).
     * @param storeTypeEnum
     *            The kind of persistence store. See {@link StoreTypeEnum}.
     * @param checker
     *            An object that is used to compute the checksum to be stored in
     *            the root block (required).
     * @param createTime
     *            The timestamp of the creation event for the journal. This is
     *            mainly used by the {@link ResourceManager} to impose a
     *            chronological order on the journals.
     * @param closeTime
     *            ZERO(0L) unless the journal has been closed for write
     *            operations, in which case this is the timestamp of the event
     *            that disallowed further writes on this journal. This is mainly
     *            used by the {@link ResourceManager} to indicate that a journal
     *            is no longer available for writing (because it has been
     *            superseded by another journal).
     */
    RootBlockView(//
            final boolean rootBlock0, final int offsetBits,
            final long nextOffset, final long firstCommitTime,
            final long lastCommitTime, final long commitCounter,
            final long commitRecordAddr, final long commitRecordIndexAddr,
            final UUID uuid,
            final long quorumToken, // VERSION2
            final long metaStartAddr, // VERSION1
            final long metaBitsAddr, // VERSION1
            final StoreTypeEnum storeTypeEnum, // VERSION1
            final long createTime, final long closeTime,
            final ChecksumUtility checker)
    {

        // Note: There is a unit test specifically for this condition.
//        if (SIZEOF_UNUSED < 0)
//            throw new RootBlockException(
//                    "Out of unused space in the root block? : " + SIZEOF_UNUSED);

        if (storeTypeEnum == null) {

            throw new IllegalArgumentException("storeType is null");

        }

        switch (storeTypeEnum) {
        case RW: {
            // @todo check metaStartAddr
            // @todo check metaBitsAddr
            am = new RWStrategy.RWAddressManager();
            // @todo check nextOffset
            break;
        }
        case WORM: {

            if (metaStartAddr != 0L)
                throw new RootBlockException("metaStartAddr must be ZERO (0L) for WORM.");
            
            if (metaBitsAddr != 0L)
                throw new RootBlockException("metaBitsAddr must be ZERO (0L) for WORM.");
            
            am = new WormAddressManager(offsetBits);
            
            WormAddressManager.assertOffsetBits(offsetBits);
            
            ((WormAddressManager) am).assertOffset(nextOffset);
            
            break;
            
        }
        default:
            throw new RootBlockException("Unknown storeType: " + storeTypeEnum);
        }
        
        if( firstCommitTime == 0L && lastCommitTime != 0L) {
         
            throw new IllegalArgumentException(
                    "first transaction identifier is zero, but last transaction identifier is not: lastCommitTime="
                            + lastCommitTime);
            
        }
        
        if (firstCommitTime != 0 && lastCommitTime < firstCommitTime) {
        
            throw new IllegalArgumentException(
                    "last transaction identifier is less than first transaction identifier: lastCommitTime="
                            + lastCommitTime
                            + ", firstCommitTime="
                            + firstCommitTime);
        }
        
        if (commitCounter < 0) {
         
            throw new IllegalArgumentException(
                    "commit counter is negative: commitCounter="
                            + commitCounter);
            
        }
        
        if (commitCounter == Long.MAX_VALUE ) {
        
            throw new IllegalArgumentException("commit counter would overflow.");
            
        }

        assertLegalAddr(am, nextOffset, commitRecordAddr, "Commit record");

        assertLegalAddr(am, nextOffset, commitRecordIndexAddr,
                "Commit record index");
        
        if (commitCounter > 0) {
            
            if (commitRecordAddr == 0) {
                
                throw new IllegalArgumentException(
                        "The commit record must exist if the commit counter is non-zero: commitCounter="
                                + commitCounter);
                
            }
            
            if (commitRecordIndexAddr == 0) {
        
                throw new IllegalArgumentException(
                        "The commit record index must exist if the commit counter is non-zero: commitCounter="
                                + commitCounter);
                
            }

        }
        
        if (commitRecordAddr != 0 && commitRecordIndexAddr == 0) {

            throw new IllegalArgumentException(
                    "The commit record index must exist if there is a commit record: commitRecordAddr="
                            + commitRecordAddr
                            + ", but commitRecordIndexAddr="
                            + commitRecordIndexAddr);
            
        }
        
        if (commitRecordIndexAddr != 0 && commitRecordAddr == 0) {

            throw new IllegalArgumentException(
                    "The commit record address must exist if there is a commit record index: commitRecordIndexAddr="
                            + commitRecordIndexAddr
                            + ", but commitRecordAddr="
                            + commitRecordAddr);
        
        }
        
        if(uuid == null) {
            
            throw new IllegalArgumentException("UUID is null");
            
        }

        /*
         * @todo Once we nail down the quorumToken assignment semantics (e.g.,
         * based on commitTime, commitCounter, pure non-negative sequences as
         * assigned by zookeeper, etc) both this test and the unit tests should
         * be modified to conform with the quorumToken validity constraints.
         */
        if (quorumToken < 0L && quorumToken != Quorum.NO_QUORUM) {

            throw new IllegalArgumentException(
                    "quorum is negative but value is not NO_QUORUM("
                            + Quorum.NO_QUORUM + ")");
            
        }
        
        if (createTime == 0L) {

            throw new IllegalArgumentException("Create time is zero.");
            
        }
        
        if (closeTime != 0L && closeTime < createTime) {
            
            throw new IllegalArgumentException(
                    "Close time proceeds create time: closeTime=" + closeTime
                            + ", createTime=" + createTime);
            
        }
        
        if(checker == null) {
            
            throw new IllegalArgumentException("Checker is null");
            
        }

        this.checker = checker;
        
        buf = ByteBuffer.allocate(SIZEOF_ROOT_BLOCK);
        
        this.rootBlock0 = rootBlock0;
        
        /*
         * This field is used to (a) identify which root block is more recent;
         * and (b) since it is written at both the start and the end of the root
         * block, to verify that the entire root block was made stable.
         * 
         * Note: I have chosen to use the commitCounter rather than a timestamp
         * field here for several reasons. First, the commitCounter is already
         * on hand and the caller has responsibility for verifying that the
         * commit counters are strictly increasing. Second, we do not have to
         * wait for another distinct timestamp. Finally, time as reported by the
         * system clock is much less reliable when all that we need is a
         * strictly increasing value on each commit.
         */
        final long challisField = commitCounter;

        /*
         * The local time at which this commit occurs.
         */
        final long localTime = System.currentTimeMillis();
        
        buf.putLong(challisField);
        buf.putInt(MAGIC);
        buf.putInt(currentVersion);
        buf.put((byte)offsetBits);
        buf.putLong(nextOffset);
        buf.putLong(localTime);
        buf.putLong(firstCommitTime);
        buf.putLong(lastCommitTime);
        buf.putLong(commitCounter);
        buf.putLong(commitRecordAddr);
        buf.putLong(commitRecordIndexAddr);
        buf.putLong(createTime);
        buf.putLong(closeTime);
        buf.position(buf.position()+SIZEOF_UNUSED); // skip unused region.
        buf.putLong(quorumToken); // VERSION2
        buf.putLong(metaBitsAddr); // VERSION1
        buf.putLong(metaStartAddr); // VERSION1
        buf.put(storeTypeEnum.getType()); // VERSION1
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        buf.putLong(challisField);
        buf.putInt(calcChecksum(checker)); // checksum of the proceeding bytes.

        assert buf.position() == SIZEOF_ROOT_BLOCK : "position="+buf.position()+" but root block is "+SIZEOF_ROOT_BLOCK+" bytes";
        assert buf.limit() == SIZEOF_ROOT_BLOCK;

        buf.position(0);
    }

    public ByteBuffer asReadOnlyBuffer() {

        return buf.asReadOnlyBuffer(); // Note: a new view onto the same data.
        
    }
    
    /**
     * Create a new read-only view from the supplied buffer.
     * 
     * @param rootBlock0
     *            There are two root blocks and they are written in an
     *            alternating order. For the sake of distinction, the first one
     *            is referred to as "rootBlock0" while the 2nd one is referred
     *            to as "rootBlock1". This parameter allows the caller to store
     *            a transient field on the view that indicates which root block
     *            it represents.
     * @param buf
     *            The buffer.
     * @param checker
     *            An object used to compute the checksum of the root block
     *            (optional, when <code>null</code> the checksum stored in the
     *            root block will not be verified).
     * 
     * @exception IllegalArgumentException
     *                if the buffer is null or if the #of bytes remaining in the
     *                buffer is not exactly {@link #SIZEOF_ROOT_BLOCK}.
     * @exception RootBlockException
     *                if the root block is not valid (bad magic, timestamps do
     *                not agree, etc).
     */
    RootBlockView(final boolean rootBlock0, final ByteBuffer buf,
            final ChecksumUtility checker) throws RootBlockException {
        
        if( buf == null ) throw new IllegalArgumentException();
        
        if( buf.remaining() != SIZEOF_ROOT_BLOCK ) {
            
            throw new IllegalArgumentException("Expecting " + SIZEOF_ROOT_BLOCK
                    + " remaining, acutal=" + buf.remaining());
            
        }

        /*
         * @todo If the buffer is modified, those changes will be immediately
         * reflected in the various methods. It probably is safer to clone the
         * buffer and then save a reference to the clone.
         */
        this.buf = buf.asReadOnlyBuffer();

        // flag indicating whether this is rootBlock ZERO or ONE.
        this.rootBlock0 = rootBlock0;

        switch (getStoreType()) {
        case RW: {
         
            am = new RWStrategy.RWAddressManager();
            
            break;
            
        }
        case WORM: {

            final int offsetBits = getOffsetBits();
            
            am = new WormAddressManager(offsetBits);
            
            break;
            
        }
        default:
            throw new RootBlockException("Unknown storeType=" + getStoreType());
        }

        if (checker == null) {
            
            log.warn("Checksum will not be validated");
            
        }

        // Check sum checker.
        this.checker = checker;

        valid();
        
    }

    public int getVersion() {
        
        return buf.getInt(OFFSET_VERSION);
        
    }

    public int getOffsetBits() {
        
        return (int) buf.get(OFFSET_OFFSET_BITS);
        
    }
    
    public long getNextOffset() {
        
        return buf.getLong(OFFSET_NEXT_OFFSET);
        
    }

    /**
     * A purely <em>informative</em> field whose value is the system local
     * timestamp of the last commit as reported by
     * {@link System#currentTimeMillis()} or the time at which the store was
     * created iff there have been no commits. Unlike
     * {@link #getFirstCommitTime()} and {@link #getLastCommitTime()} this field
     * does NOT provide a guarantee that time is strictly increasing.
     * <p>
     * Note: This field is stored for post-mortem purposes and is NOT used
     * internally.
     * 
     * @return A purely <em>informative</em> field whose value is the system
     *         local timestamp of the last commit.
     */
    public long getLocalTime() {
        
        return buf.getLong(OFFSET_LOCAL_TIME);
        
    }
    
    public long getFirstCommitTime() {
        
        return buf.getLong(OFFSET_FIRST_CMIT);
        
    }
    
    public long getLastCommitTime() {
        
        return buf.getLong(OFFSET_LAST_CMIT);
        
    }
    
    /**
     * The "Challis field" is written at both the head and the tail of each root
     * block and is a strictly increasing value positive long integer. This
     * field is used to detect partial writes of the root blocks (the values of
     * the field in a given root block will not agree).  This field is also used
     * to identify which of the two root blocks is more recent (it will have the
     * larger value for this field).
     * 
     * @return The value of the "Challis" field.
     * 
     * @throws RootBlockException
     *             if the values on the root block do not agree.
     */
    public long getChallisField() throws RootBlockException {
        
        final long timestamp0 = buf.getLong(OFFSET_CHALLIS0);
        
        final long timestamp1 = buf.getLong(OFFSET_CHALLIS1);
        
        if( timestamp0 != timestamp1 ) {
            
            throw new RootBlockException("Timestamps differ: "+timestamp0 +" vs "+ timestamp1);
            
        }
        
        return timestamp0;
        
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

            throw new RootBlockException("MAGIC: expected="+MAGIC+", actual="+magic);
            
        }

        final int version = buf.getInt(OFFSET_VERSION);

        switch (version) {
        case VERSION0:
        case VERSION1:
        case VERSION2:
            break;
        default:
            throw new RootBlockException("Unknown version: " + version);
        }
        
        // test that the store checksum field can be validated against the stored data.
        getChecksum(checker);
        
        // test that the "Challis" fields agree.
        getChallisField();
        
        if(getCreateTime() == 0L) {

            throw new RootBlockException("Create time should not be zero.");
            
        }

    }

    public UUID getUUID() {
        
        return new UUID(//
                buf.getLong(OFFSET_UUID), // MSB
                buf.getLong(OFFSET_UUID + Bytes.SIZEOF_LONG) //LSB
                );
        
    }

    public long getCreateTime() {
        
        return buf.getLong(OFFSET_CREATE_TIME);
        
    }
    
    public long getCloseTime() {
        
        return buf.getLong(OFFSET_CLOSE_TIME);
        
    }
    
    /**
     * Return the checksum store in the root block (excluding only the field
     * including the checksum value itself).
     * 
     * @param checker
     *            An object used to compute the checksum. Note that you can
     *            disable the checksum test by passing in a <code>null</code>
     *            value here.
     * 
     * @return The checksum stored in the root block.
     * 
     * @exception RootBlockException
     *                if the checksum stored in the root block does not agree
     *                with the checksum computed by
     *                {@link #calcChecksum(ChecksumUtility)}.
     */
    public int getChecksum(final ChecksumUtility checker) throws RootBlockException {

        // the value stored in the root block.
        final int storedChecksum = buf.getInt(OFFSET_CHECKSUM);

        if (checker == null) {
            
            log.info("Checksum not validated");

            return storedChecksum;

        }

        // compute the checksum of the root block.
        final int actualChecksum = calcChecksum(checker);

        if (storedChecksum != 0) {

            if (storedChecksum != actualChecksum) {

                throw new RootBlockException("Checksum error");

            }

        }

        return storedChecksum;

    }

    /**
     * Compute the checksum of the root block (excluding only the field
     * including the checksum value itself).
     */
    public int calcChecksum(final ChecksumUtility checker) {
        
        if (checker == null)
            throw new IllegalArgumentException();
        
        return checker.checksum(buf, 0, SIZEOF_ROOT_BLOCK - SIZEOF_CHECKSUM);
        
    }

    public String toString() {
    
        StringBuilder sb = new StringBuilder();
        
        sb.append("rootBlock");
        
        sb.append("{ rootBlock="+(isRootBlock0()?0:1));
        sb.append(", challisField="+getChallisField());
        sb.append(", version="+getVersion());
        sb.append(", nextOffset="+getNextOffset());
        sb.append(", localTime="+getLocalTime()+"["+new Date(getLocalTime())+"]");
        sb.append(", firstCommitTime="+getFirstCommitTime()+"["+new Date(getFirstCommitTime())+"]");
        sb.append(", lastCommitTime="+getLastCommitTime()+"["+new Date(getLastCommitTime())+"]");
        sb.append(", commitCounter="+getCommitCounter());
        sb.append(", commitRecordAddr="+am.toString(getCommitRecordAddr()));
        sb.append(", commitRecordIndexAddr="+am.toString(getCommitRecordIndexAddr()));
        sb.append(", quorumToken="+getQuorumToken());
        sb.append(", metaBitsAddr=" + getMetaBitsAddr());
        sb.append(", metaStartAddr=" + getMetaStartAddr());
        sb.append(", storeType="+getStoreType().getType());
        sb.append(", uuid="+getUUID());
        sb.append(", offsetBits="+getOffsetBits());
        sb.append(", checksum="+(checker==null?"N/A":""+calcChecksum(checker)));
        sb.append(", createTime="+getCreateTime()+"["+new Date(getCreateTime())+"]");
        sb.append(", closeTime="+getCloseTime()+(getCloseTime()!=0L?"["+new Date(getCloseTime())+"]":""));
        
        sb.append("}");
        
        return sb.toString();
        
    }

    public long getMetaBitsAddr() {
        if (getVersion() < VERSION1) {
            // Always WORM store before VERSION1
            return 0L;
        }
        return buf.getLong(OFFSET_META_BITS);
    }

    public long getMetaStartAddr() {
        if (getVersion() < VERSION1) {
            // Always WORM store before VERSION1
            return 0L;
        }
        return buf.getLong(OFFSET_META_START);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: The {@link #OFFSET_STORETYPE} field was defined in
     * {@link #VERSION1}. The default value for that field before
     * {@link #VERSION1} is {@link StoreTypeEnum#WORM} since only the WORM
     * persistence store existed in earlier versions of the root block.
     */
    public StoreTypeEnum getStoreType() {
        if (getVersion() < VERSION1) {
            // Always WORM store before VERSION1
            return StoreTypeEnum.WORM;
        }
        return StoreTypeEnum.valueOf(buf.get(OFFSET_STORETYPE));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: The quorum token was introduced in {@link #VERSION2}. The default
     * value for that field before {@link #VERSION2} is {Quorum#NO_QUORUM}.
     */
    public long getQuorumToken() {
        if (getVersion() < VERSION2) {
            return Quorum.NO_QUORUM;
        }
        return buf.getLong(OFFSET_QTOKEN);
    }

}
