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
package com.bigdata.btree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Date;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IAddressManager;

/**
 * The checkpoint record for an {@link IndexSegment}.
 * <p>
 * The checkpoint record for the index segment file is written at the head of
 * the file. It should have identical timestamps at the start and end of the
 * checkpoint record (e.g., it doubles as a root block). Since the file format
 * is immutable it is ok to have what is essentially only a single root block.
 * If the timestamps do not agree then the build was not successfully completed.
 * <p>
 * Similar to the {@link BTree}'s {@link Checkpoint} record, this record
 * contains only data that pertains specifically to the {@link IndexSegment}
 * checkpoint or data otherwise required to bootstrap the load of the
 * {@link IndexSegment} from the file. General purpose metadata is stored in the
 * extension metadata record.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo the checksum of the index segment file should be stored in the
 *       partitioned index so that it can be validated after being moved around,
 *       etc. If we do this then it might not be necessary to checksum the
 *       individual records within the {@link IndexSegment}.
 *       <p>
 *       it would also be good to checksum the {@link IndexSegmentCheckpoint}
 *       record.
 */
public class IndexSegmentCheckpoint {

    /**
     * Logger.
     */
    protected static final Logger log = Logger
            .getLogger(IndexSegmentCheckpoint.class);

    static final int SIZEOF_MAGIC = Bytes.SIZEOF_INT;
    static final int SIZEOF_VERSION = Bytes.SIZEOF_INT;
    static final int SIZEOF_OFFSET_BITS = Bytes.SIZEOF_BYTE;
    static final int SIZEOF_BRANCHING_FACTOR = Bytes.SIZEOF_INT;
    static final int SIZEOF_COUNTS = Bytes.SIZEOF_INT;
    static final int SIZEOF_NBYTES = Bytes.SIZEOF_INT;
    static final int SIZEOF_ADDR = Bytes.SIZEOF_LONG;
    static final int SIZEOF_ERROR_RATE = Bytes.SIZEOF_DOUBLE;
    static final int SIZEOF_TIMESTAMP = Bytes.SIZEOF_LONG;

    /**
     * The #of unused bytes in the checkpoint record format. Note that the
     * unused space occurs <em>before</em> the final timestamp in the record.
     * As the unused bytes are allocated in new versions the value in this field
     * MUST be adjusted down from its original value of 256.
     */
    static final int SIZEOF_UNUSED = 256;

    /**
     * The #of bytes required by the current metadata record format.
     */
    static final int SIZE = //
            SIZEOF_MAGIC + //
            SIZEOF_VERSION + //
            Bytes.SIZEOF_LONG + // timestamp0
            Bytes.SIZEOF_UUID + // segment UUID.
            SIZEOF_OFFSET_BITS + // #of bits used to represent a byte offset.
            SIZEOF_COUNTS * 4 + // height, #leaves, #nodes, #entries
            SIZEOF_NBYTES + // max record length
            SIZEOF_ADDR * 6 + // leaves, nodes, root, metadata, bloomFilter, blobs
            Bytes.SIZEOF_LONG + // file size
            SIZEOF_UNUSED + // available bytes for future versions.
            Bytes.SIZEOF_LONG // timestamp1
    ;
    
    /**
     * Magic value written at the start of the metadata record.
     */
    static transient final public int MAGIC = 0x87ab34f5;
    
    /**
     * Version 0 of the serialization format.
     */
    static transient final public int VERSION0 = 0x0;
   
    /**
     * UUID for this {@link IndexSegment} (it is a unique identifier for
     * the index segment resource).
     */
    final public UUID segmentUUID;

    /**
     * The #of bits in an 64-bit long integer address that are used to represent
     * the byte offset into the {@link IndexSegmentFileStore}.
     */
    final public int offsetBits;

    /**
     * The {@link IAddressManager} used to interpret addresses in the
     * {@link IndexSegmentFileStore}.
     */
    final IndexSegmentAddressManager am;
    
    /**
     * Height of the index segment (origin zero, so height := 0 means that
     * there is only a root leaf in the tree).
     */
    final public int height;
    
    /**
     * The #of leaves serialized in the file.
     */
    final public int nleaves;
    
    /**
     * The #of nodes serialized in the file. If zero, then
     * {@link #nleaves} MUST be ONE (1) and the index consists solely of
     * a root leaf.
     */
    final public int nnodes;
    
    /**
     * The #of index entries serialized in the file. This must be a
     * positive integer as an empty index is not permitted (this forces
     * the application to check the btree and NOT build the index
     * segment when it is empty).
     */
    final public int nentries;
    
    /**
     * The maximum #of bytes in any node or leaf stored on the index
     * segment.
     * 
     * @todo this appears to be unused now - perhaps it should go away?
     */
    final public int maxNodeOrLeafLength;
    
    /**
     * The address of the contiguous region containing the serialized leaves in
     * the file.
     * <p>
     * Note: The offset component of this address must be equal to {@link #SIZE}
     * since the leaves are written immediately after the
     * {@link IndexSegmentCheckpoint} record.
     */
    final public long addrLeaves;
    
    /**
     * The address of the contiguous region containing the serialized nodes in
     * the file or <code>0L</code> iff there are no nodes in the file.
     */
    final public long addrNodes;
    
    /**
     * Address of the root node or leaf in the file.
     */
    final public long addrRoot;

    /**
     * The address of the {@link IndexMetadata} record.
     */
    final public long addrMetadata;
    
    /**
     * Address of the optional bloom filter and 0L iff no bloom filter
     * was constructed.
     */
    final public long addrBloom;
    
    /**
     * Address of the optional records to be resolved by blob references.
     */
    final public long addrBlobs;
    
    /**
     * Length of the file in bytes.
     */
    final public long length;
    
    /**
     * The commit time associated with the view from which the
     * {@link IndexSegment} was generated. This field is written at the head and
     * tail of the {@link IndexSegmentCheckpoint} record. If the timestamps on
     * that record do not agree then the build operation probably failed while
     * writing the checkpoint record.
     */
    final public long commitTime;

    /**
     * Reads the metadata record for the {@link IndexSegment} from the current
     * position in the file.
     * 
     * @param raf
     *            The file.
     * 
     * @throws IOException
     */
    public IndexSegmentCheckpoint(RandomAccessFile raf) throws IOException {

//        raf.seek(0);
        
        final int magic = raf.readInt();

        if (magic != MAGIC) {
            throw new IOException("Bad magic: actual=" + magic
                    + ", expected=" + MAGIC);
        }

        final int version = raf.readInt();
        
        if( version != VERSION0 ) {
            
            throw new IOException("unknown version="+version);
            
        }

        final long timestamp0 = raf.readLong();
        
        segmentUUID = new UUID(raf.readLong()/*MSB*/, raf.readLong()/*LSB*/);

        offsetBits = raf.readByte();
        
        height = raf.readInt();
        
        nleaves = raf.readInt();
        
        nnodes = raf.readInt();
        
        nentries = raf.readInt();

        maxNodeOrLeafLength = raf.readInt();
        
        addrLeaves = raf.readLong();

        addrNodes = raf.readLong();
        
        addrRoot = raf.readLong();

        addrMetadata = raf.readLong();

        addrBloom = raf.readLong();
        
        addrBlobs = raf.readLong();
        
        length = raf.readLong();
        
        if (length != raf.length()) {

            throw new IOException("Length differs: actual=" + raf.length()
                    + ", expected=" + length);

        }
        
        raf.skipBytes(SIZEOF_UNUSED);
        
        final long timestamp1 = raf.readLong();
        
        if(timestamp0 != timestamp1) {
            
            throw new RuntimeException("Timestamps do not agree - file is not useable.");
            
        }
        
        this.commitTime = timestamp0;
        
        am = new IndexSegmentAddressManager(this); 
        
        validate();

        log.info(this.toString());

    }

    /**
     * Create a new metadata record in preparation for writing it on a file
     * containing a newly constructed {@link IndexSegment}.
     * 
     * @todo javadoc.
     */
    public IndexSegmentCheckpoint(int offsetBits, int height, int nleaves,
            int nnodes, int nentries, int maxNodeOrLeafLength, long addrLeaves,
            long addrNodes, long addrRoot, long addrMetadata, long addrBloom,
            long addrBlobs, long length, UUID segmentUUID, long commitTime) {

        /*
         * Copy the various fields to initialize the checkpoint record.
         */
        
        this.segmentUUID = segmentUUID;

        this.offsetBits = offsetBits;
        
        this.height = height;

        this.nleaves = nleaves;
        
        this.nnodes = nnodes;
        
        this.nentries = nentries;

        this.maxNodeOrLeafLength = maxNodeOrLeafLength;
        
        this.addrLeaves = addrLeaves;
        
        this.addrNodes = addrNodes;
        
        this.addrRoot = addrRoot;

        this.addrMetadata = addrMetadata;

        this.addrBloom = addrBloom;

        this.addrBlobs = addrBlobs;
        
        this.length = length;
        
        this.commitTime = commitTime;
        
        /*
         * Create the address manager using this checkpoint record (requires
         * that certain fields are initialized on the checkpoint record).
         */
        
        am = new IndexSegmentAddressManager(this);
        
        validate();
        
        log.info(this.toString());
        
    }

    /**
     * Test validity of the checkpoint record.
     * 
     * @todo this uses asserts which may be disabled.
     */
    public void validate() {
        
//        assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
        
        assert height >= 0;
        
        assert nleaves > 0; // always at least one leaf.
        
        assert nnodes >= 0; // may be just a root leaf.
        
        // index may not be empty.
        assert nentries > 0;
//        // #entries must fit within the tree height.
//        assert nentries <= Math.pow(branchingFactor,height+1);
        
        assert maxNodeOrLeafLength > 0;
        
        assert addrLeaves != 0L;
        assert am.getOffset(addrLeaves)==SIZE;
        assert am.getByteCount(addrLeaves) > 0;

        if(nnodes == 0) {
            // the root is a leaf.
            assert addrNodes == 0L;
            assert am.getOffset(addrLeaves) < length;
            assert am.getOffset(addrRoot) >= am.getOffset(addrLeaves);
            assert am.getOffset(addrRoot) < length;
        } else {
            // the root is a node.
            assert am.getOffset(addrNodes) > am.getOffset(addrLeaves);
            assert am.getOffset(addrNodes) < length;
            assert am.getOffset(addrRoot) >= am.getOffset(addrNodes);
            assert am.getOffset(addrRoot) < length;
        }

        /*
         * @todo validate the blob, bloom, and metadata addresses as well and
         * the total length of the file.
         */
        
//        if( addrBloom == 0L ) assert errorRate == 0.;
//        
//        if( errorRate != 0.) assert addrBloom != 0L;
        
        assert commitTime != 0L;
        
        assert segmentUUID != null;

    }
    
    /**
     * Write the metadata record on the current position of the file.
     * 
     * @param raf
     *            The file.
     *            
     * @throws IOException
     */
    public void write(RandomAccessFile raf) throws IOException {

//        raf.seek(0);

        log.info("wrote: pos="+raf.getFilePointer());

        raf.writeInt(MAGIC);

        raf.writeInt(VERSION0);

        raf.writeLong(commitTime);
        
        raf.writeLong(segmentUUID.getMostSignificantBits());

        raf.writeLong(segmentUUID.getLeastSignificantBits());

        raf.writeByte(offsetBits);
                        
        raf.writeInt(height);
        
        raf.writeInt(nleaves);

        raf.writeInt(nnodes);
        
        raf.writeInt(nentries);

        raf.writeInt(maxNodeOrLeafLength);
        
        raf.writeLong(addrLeaves);
        
        raf.writeLong(addrNodes);

        raf.writeLong(addrRoot);

        raf.writeLong(addrMetadata);
        
        raf.writeLong(addrBloom);
        
        raf.writeLong(addrBlobs);
        
        raf.writeLong(length);

        /*
         * skip over this many bytes. Note that skipBytes() does not seem to
         * really skip over bytes when writing while this approach definately
         * writes out those bytes and advances the file pointer.  seek() also
         * works.
         */
//        raf.skipBytes(SIZEOF_UNUSED);
//        raf.write(new byte[SIZEOF_UNUSED]);
        raf.seek(raf.getFilePointer()+SIZEOF_UNUSED);
        
        raf.writeLong(commitTime);

        log.info("wrote: pos="+raf.getFilePointer());
        
    }
    
    /**
     * A human readable representation of the metadata record.
     */
    public String toString() {
 
        StringBuilder sb = new StringBuilder();
        
        sb.append("magic="+Integer.toHexString(MAGIC));
        sb.append(", segmentUUID="+segmentUUID);
        sb.append(", offsetBits="+offsetBits);
        sb.append(", height=" + height);
        sb.append(", nleaves=" + nleaves);
        sb.append(", nnodes=" + nnodes);
        sb.append(", nentries=" + nentries);
        sb.append(", maxNodeOrLeafLength=" + maxNodeOrLeafLength);
        sb.append(", addrLeaves=" + am.toString(addrLeaves));
        sb.append(", addrNodes=" + am.toString(addrNodes));
        sb.append(", addrRoot=" + am.toString(addrRoot));
        sb.append(", addrMetadata=" + am.toString(addrMetadata));
        sb.append(", addrBloom=" + am.toString(addrBloom));
        sb.append(", addrBlobs=" + am.toString(addrBlobs));
        sb.append(", length=" + length);
        sb.append(", commitTime=" + new Date(commitTime));

        return sb.toString();
    }

}
