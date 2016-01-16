/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

package com.bigdata.ha.msg;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.io.compression.CompressorRegistry;
import com.bigdata.io.compression.IRecordCompressor;
import com.bigdata.journal.StoreTypeEnum;

/**
 * A message carrying RMI metadata about a payload which will be replicated
 * using a socket-level transfer facility.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HAWriteMessage extends HAWriteMessageBase implements
        IHAWriteMessage {

    protected static final Logger log = Logger.getLogger(HAWriteMessage.class);

    /**
     * 
     */
    private static final long serialVersionUID = -2673171474897401979L;

    /** The {@link UUID} of the store to which this message belongs. */
    private UUID uuid;
    
    /** The most recent commit counter associated with this message */
    private long commitCounter;

    /** The most recent commit time associated with this message */
    private long lastCommitTime;

    /** The write sequence since last commit beginning at zero */
	private long sequence;

	   /** The type of backing store (RW or WORM). */
    private StoreTypeEnum storeType;
    
    /** Indicates if data is compressed (if included in file). */
    private String compressorKey;

    /** The quorum token for which this message is valid. */
    private long quorumToken;

    /** The replication factor for the quorum leader. */
    private int replicationFactor;

    /** The length of the backing file on the disk. */
    private long fileExtent;

    /** The file offset at which the data will be written (WORM only). */
    private long firstOffset;

    @Override
    public UUID getUUID() {
        return uuid;
    }

    /* (non-Javadoc)
     * @see com.bigdata.journal.ha.IHAWriteMessage#getCommitCounter()
     */
    @Override
    public long getCommitCounter() {
        return commitCounter;
    }
    
    /* (non-Javadoc)
     * @see com.bigdata.journal.ha.IHAWriteMessage#getLastCommitTime()
     */
    @Override
    public long getLastCommitTime() {
        return lastCommitTime;
    }

    /* (non-Javadoc)
     * @see com.bigdata.journal.ha.IHAWriteMessage#getSequence()
     */
    @Override
    public long getSequence() {
        return sequence;
    }
    
    
    
    /* (non-Javadoc)
     * @see com.bigdata.journal.ha.IHAWriteMessage#getStoreType()
     */
    @Override
    public StoreTypeEnum getStoreType() {
        return storeType;
    }
    
    /* (non-Javadoc)
     * @see com.bigdata.journal.ha.IHAWriteMessage#getQuorumToken()
     */
    @Override
    public long getQuorumToken() {
        return quorumToken; 
    }
    
    @Override
    public int getReplicationFactor() {
        return replicationFactor; 
    }
    
    /* (non-Javadoc)
     * @see com.bigdata.journal.ha.IHAWriteMessage#getFileExtent()
     */
    @Override
    public long getFileExtent() {
        return fileExtent; 
    }
    
    /* (non-Javadoc)
     * @see com.bigdata.journal.ha.IHAWriteMessage#getFirstOffset()
     */
    @Override
    public long getFirstOffset() {
        return firstOffset; 
    }
    
    @Override
    public String getCompressorKey() {
        return compressorKey;
    }

    @Override
 	public String toString() {

        return getClass().getName() //
                + "{size=" + getSize() //
                + ",chksum=" + getChk() //
                + ",uuid="+getUUID() // 
                + ",commitCounter=" + commitCounter //
                + ",commitTime=" + lastCommitTime //
                + ",sequence=" + sequence //
                + ",storeType=" + getStoreType() //
                + ",compressorKey=" + getCompressorKey() //
                + ",quorumToken=" + getQuorumToken()//
                + ",replicationFactor=" + getReplicationFactor() //
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
     * @param uuid
     *            The {@link UUID} associated with the backing store on the
     *            leader. This can be used to decide whether the message is for
     *            a given store, or (conversly) whether the receiver has already
     *            setup its root blocks based on the leader (and hence has the
     *            correct {@link UUID} for its local store).
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
     * 
     * @deprecated by the version that accepts the compressor key.
     */
    public HAWriteMessage(final UUID uuid, final long commitCounter,
            final long commitTime, final long sequence, final int sze,
            final int chk, final StoreTypeEnum storeType,
            final long quorumToken, final long fileExtent,
            final long firstOffset) {

        this(uuid, commitCounter, commitTime, sequence, sze, chk, storeType,
                quorumToken, 0/* replicationFactor */, fileExtent, firstOffset,
                null/* compressorKey */);

    }

    /**
     * @param uuid
     *            The {@link UUID} associated with the backing store on the
     *            leader. This can be used to decide whether the message is for
     *            a given store, or (conversly) whether the receiver has already
     *            setup its root blocks based on the leader (and hence has the
     *            correct {@link UUID} for its local store).
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
     * @param replicationFactor
     *            The replication factor in effect for the leader.
     * @param fileExtent
     *            The length of the backing file on the disk.
     * @param firstOffset
     *            The file offset at which the data will be written (WORM only).
     * @param compressorKey
     *            The key under which an {@link IRecordCompressor} has been
     *            registered against the {@link CompressorRegistry} -or-
     *            <code>null</code> for no compression.
     */
    public HAWriteMessage(final UUID uuid, final long commitCounter,
                final long commitTime, final long sequence, final int sze,
                final int chk, final StoreTypeEnum storeType,
                final long quorumToken, 
                final int replicationFactor,
                final long fileExtent,
                final long firstOffset,
                final String compressorKey) {

        super(sze, chk);

        if (uuid == null)
            throw new IllegalArgumentException();
        
        if (storeType == null)
            throw new IllegalArgumentException();
        
        this.uuid = uuid;
        
        this.commitCounter = commitCounter;
        
        this.lastCommitTime = commitTime;
        
        this.sequence = sequence;
        
        this.storeType = storeType;
        
        this.quorumToken = quorumToken;
        
        this.fileExtent = fileExtent;
        
        this.firstOffset = firstOffset;
      
        this.replicationFactor = replicationFactor;
        
        this.compressorKey = compressorKey;
        
    }

    /**
     * The initial version.
     * 
     * Note: We have never done a release with this version number. It should
     * only exist for HALog files an HAJournal files for the branch in which we
     * were doing development on the HAJournalServer. In all released versions,
     * the {@link #uuid} field should always be non-<code>null</code>.
     */
    private static final byte VERSION0 = 0x0;

    /**
     * Adds the {@link #uuid} field.
     */
    private static final byte VERSION1 = 0x1;

    /**
     * Supports optional data compression for the payload (backwards compatible
     * default for {@link #VERSION1} is no compression).
     */
    private static final byte VERSION2 = 0x2;

    /**
     * Adds the {@link #replicationFactor} for the quorum leader (decodes as
     * ZERO (0) for older versions).
     */
    private static final byte VERSION3 = 0x3;
    
    /**
     * The current version.
     */
    private static final byte currentVersion = VERSION3;
    
    /**
     * Determine whether message data is compressed
     */
    private static boolean compressData = true; // default
    
    /**
     * Static method to indicate whether the message will reference
     * compressed data.
     * 
     * @return
     */
    public static boolean isDataCompressed() {

        return compressData;
        
    }
    
    @Override
    public boolean equals(final Object obj) {

        if (this == obj)
            return true;
        
        if (!super.equals(obj))
            return false;

        if (!(obj instanceof IHAWriteMessage))
            return false;
        
        final IHAWriteMessage other = (IHAWriteMessage) obj;

        return ((uuid == null && other.getUUID() == null) || uuid.equals(other.getUUID()))
                && commitCounter == other.getCommitCounter()
                && lastCommitTime == other.getLastCommitTime() //
                && sequence == other.getSequence()
                && storeType == other.getStoreType()
                && quorumToken == other.getQuorumToken()
                && fileExtent == other.getFileExtent()
                && firstOffset == other.getFirstOffset();

    }

    @Override
    public void readExternal(final ObjectInput in) throws IOException,
			ClassNotFoundException {

		super.readExternal(in);

	    final byte version = in.readByte();
		switch (version) {
		case VERSION0:
		    uuid = null; // Note: not available.
			break;
		case VERSION3: // fall through
        case VERSION2: {
            final boolean isNull = in.readBoolean();
            compressorKey = isNull ? null : in.readUTF();
            // fall through.
        }
        case VERSION1:
            uuid = new UUID(//
                    in.readLong(), // MSB
                    in.readLong() // LSB
            );
            break;
		default:
			throw new IOException("Unknown version: " + version);
		}
		storeType = StoreTypeEnum.valueOf(in.readByte());
		commitCounter = in.readLong();
		lastCommitTime = in.readLong();
		sequence = in.readLong();
		quorumToken = in.readLong();
        if (version >= VERSION3) {
            replicationFactor = in.readInt();
        } else {
            replicationFactor = 0;
        }
		fileExtent = in.readLong();
		firstOffset = in.readLong();
	}

    @Override
	public void writeExternal(final ObjectOutput out) throws IOException {
		super.writeExternal(out);
        if (currentVersion >= VERSION1 && uuid != null) {
            out.write(currentVersion);
            if (currentVersion >= VERSION2) {
                out.writeBoolean(compressorKey == null);
                if (compressorKey != null)
                    out.writeUTF(compressorKey);
            }
            out.writeLong(uuid.getMostSignificantBits());
            out.writeLong(uuid.getLeastSignificantBits());
        } else {
            // Note: Replay of an older log message.
            out.write(VERSION0);
        }
		out.writeByte(storeType.getType());
		out.writeLong(commitCounter);
		out.writeLong(lastCommitTime);
		out.writeLong(sequence);
		out.writeLong(quorumToken);
        if (currentVersion >= VERSION3)
            out.writeInt(replicationFactor);
		out.writeLong(fileExtent);
		out.writeLong(firstOffset);
	}

//	// Versions of compress/expand with Deflator using RecordCompressor
//	static IRecordCompressor compressor = CompressorRegistry.fetch(CompressorRegistry.DEFLATE_BEST_SPEED);
//	static String compressorKey = CompressorRegistry.DEFLATE_BEST_SPEED;
	
//	/**	
//	 * This configuration method has a dual role since if the Deflater is configured
//	 * with NO_COMPRESSION, the message indicates directly that the buffer is not compressed
//	 * avoiding the double buffering of the Deflater class.
//	 * 
//	 * Note that the strategy is only applicable for the compression, the expansion is
//	 * determined by the source data.
//	 */
//	 public static void setCompression(final String strategy) {
//		 compressorKey = strategy;
//		 compressor = CompressorRegistry.fetch(strategy);
//	}
//	
//	public ByteBuffer compress(final ByteBuffer buffer) {
//
//        final IRecordCompressor compressor = CompressorRegistry.getInstance()
//                .get(compressionMethod);
//
//        if (compressor == null)
//            throw new UnsupportedOperationException("Unknown compressor: "
//                    + compressionMethod);
//
//        return compressor.compress(buffer);
//    }

    @Override
    public ByteBuffer expand(final ByteBuffer buffer) {

        final String compressorKey = getCompressorKey();
        
        if (compressorKey == null) {

            /*
             * No compression.
             */

            return buffer;

        }

        final IRecordCompressor compressor = CompressorRegistry.getInstance()
                .get(compressorKey);

        if (compressor == null)
            throw new UnsupportedOperationException("Unknown compressor: "
                    + compressorKey);

        return compressor.decompress(buffer);
        
    }

    // public static IRecordCompressor getCompressor() {
    // return compressor;
    // }
}
