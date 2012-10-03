/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on May 18, 2010
 */

package com.bigdata.journal.ha;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.ha.pipeline.HAWriteMessageBase;
import com.bigdata.journal.StoreTypeEnum;

/**
 * A message carrying RMI metadata about a payload which will be replicated
 * using a socket-level transfer facility.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HAWriteMessage extends HAWriteMessageBase {

    /**
     * 
     */
    private static final long serialVersionUID = -2673171474897401979L;

    /** The most recent commit counter associated with this message */
    private long commitCounter;

    /** The most recent commit time associated with this message */
    private long lastCommitTime;

    /** The write sequence since last commit beginning at zero */
	private long sequence;

   /** The type of backing store (RW or WORM). */
    private StoreTypeEnum storeType;

    /** The quorum token for which this message is valid. */
    private long quorumToken;

    /** The length of the backing file on the disk. */
    private long fileExtent;

    /** The file offset at which the data will be written (WORM only). */
    private long firstOffset;

    /** The commit counter associated with this message */
    public long getCommitCounter() {
        return commitCounter;
    }
    
    /** The commit time associated with this message. */
    public long getLastCommitTime() {
        return lastCommitTime;
    }

    /**
     * The write cache buffer sequence number (reset to ZERO (0) for the first
     * message after each commit and incremented for each buffer sent by the
     * leader).
     */
    public long getSequence() {
        return sequence;
    }
    
    /** The type of backing store (RW or WORM). */
    public StoreTypeEnum getStoreType() {
        return storeType;
    }
    
    /** The quorum token for which this message is valid. */
    public long getQuorumToken() {
        return quorumToken; 
    }
    
    /** The length of the backing file on the disk. */
    public long getFileExtent() {
        return fileExtent; 
    }
    
    /** The file offset at which the data will be written (WORM only). */
    public long getFirstOffset() {
        return firstOffset; 
    }
    
    public String toString() {

        return getClass().getName() //
                + "{size=" + getSize() //
                + ",chksum=" + getChk() //
                + ",commitCounter=" + commitCounter //
                + ",commitTime=" + lastCommitTime //
                + ",sequence=" + sequence //
                + ",storeType=" + getStoreType() //
                + ",quorumToken=" + getQuorumToken()//
                + ",fileExtent=" + getFileExtent() //
                + ",firstOffset=" + getFirstOffset() //
                + "}";

    }

    /**
     * De-serialization constructor.
     */
    public HAWriteMessage() {
    }

    /**
     * @param commitCounter
     *            The commit counter for the current root block for the write
     *            set which is being replicated by this message.
     * @param commitTime
     *            The commit time for the current root block for the write set
     *            which is being replicated by this message.
     * @param sequence
     *            The write cache block sequence number. This is reset to ZERO
     *            (0) for the first replicated write cache block in each write
     *            set.
     * @param sze
     *            The #of bytes in the payload.
     * @param chk
     *            The checksum of the payload.
     * @param storeType
     *            The type of backing store (RW or WORM).
     * @param quorumToken
     *            The quorum token for which this message is valid.
     * @param fileExtent
     *            The length of the backing file on the disk.
     * @param firstOffset
     *            The file offset at which the data will be written (WORM only).
     */
    public HAWriteMessage(final long commitCounter, final long commitTime,
            final long sequence, final int sze, final int chk,
            final StoreTypeEnum storeType, final long quorumToken,
            final long fileExtent, final long firstOffset) {

        super(sze, chk);

        if (storeType == null)
            throw new IllegalArgumentException();
        
        this.commitCounter = commitCounter;
        
        this.lastCommitTime = commitTime;
        
        this.sequence = sequence;
        
        this.storeType = storeType;
        
        this.quorumToken = quorumToken;
        
        this.fileExtent = fileExtent;
        
        this.firstOffset = firstOffset;
        
    }

    private static final byte VERSION0 = 0x0;

    @Override
    public boolean equals(final Object obj) {

        if (this == obj)
            return true;
        
        if (!super.equals(obj))
            return false;

        if (!(obj instanceof HAWriteMessage))
            return false;
        
        final HAWriteMessage other = (HAWriteMessage) obj;

        return commitCounter == other.getCommitCounter()
                && lastCommitTime == other.getLastCommitTime() //
                && sequence == other.getSequence()
                && storeType == other.getStoreType()
                && quorumToken == other.getQuorumToken()
                && fileExtent == other.getFileExtent()
                && firstOffset == other.getFirstOffset();

    }

    public void readExternal(final ObjectInput in) throws IOException,
			ClassNotFoundException {

		super.readExternal(in);
		final byte version = in.readByte();
		switch (version) {
		case VERSION0:
			break;
		default:
			throw new IOException("Unknown version: " + version);
		}
		storeType = StoreTypeEnum.valueOf(in.readByte());
		commitCounter = in.readLong();
		lastCommitTime = in.readLong();
		sequence = in.readLong();
		quorumToken = in.readLong();
		fileExtent = in.readLong();
		firstOffset = in.readLong();
	}

	public void writeExternal(final ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.write(VERSION0);
		out.writeByte(storeType.getType());
		out.writeLong(commitCounter);
		out.writeLong(lastCommitTime);
		out.writeLong(sequence);
		out.writeLong(quorumToken);
		out.writeLong(fileExtent);
		out.writeLong(firstOffset);
	}

}
