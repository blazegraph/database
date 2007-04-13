package com.bigdata.btree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Date;
import java.util.UUID;

import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.Bytes;

/**
 * The metadata record for an {@link IndexSegment}.
 * <p>
 * The commit point for the index segment file should be a metadata record at
 * the head of the file having identical timestamps at the start and end of its
 * data section. Since the file format is immutable it is ok to have what is
 * essentially only a single root block. If the timestamps do not agree then the
 * build was not successfully completed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo the checksum of the index segment file should be stored in the
 *       partitioned index so that it can be validated after being moved around,
 *       etc. it would also be good to checksum the {@link IndexSegmentMetadata}
 *       record.
 */
public class IndexSegmentMetadata {

    static final int SIZEOF_MAGIC = Bytes.SIZEOF_INT;
    static final int SIZEOF_VERSION = Bytes.SIZEOF_INT;
    static final int SIZEOF_BRANCHING_FACTOR = Bytes.SIZEOF_INT;
    static final int SIZEOF_COUNTS = Bytes.SIZEOF_INT;
    static final int SIZEOF_NBYTES = Bytes.SIZEOF_INT;
    static final int SIZEOF_ADDR = Bytes.SIZEOF_LONG;
    static final int SIZEOF_ERROR_RATE = Bytes.SIZEOF_DOUBLE;
    static final int SIZEOF_TIMESTAMP = Bytes.SIZEOF_LONG;

    /**
     * The #of unused bytes in the metadata record format. Note that the unused
     * space occurs between the file size and the final timestamp in the record.
     * As the unused bytes are allocated in new versions the value in this field
     * MUST be adjusted down from its original value of 256.
     */
    static final int SIZEOF_UNUSED = 240;

    /**
     * The #of bytes required by the current metadata record format.
     */
    static final int SIZE = //
            SIZEOF_MAGIC + //
            SIZEOF_VERSION + //
            Bytes.SIZEOF_LONG + // timestamp0
            Bytes.SIZEOF_UUID + // segment UUID.
            SIZEOF_BRANCHING_FACTOR + // branchingFactor
            SIZEOF_COUNTS * 4 + // height, #leaves, #nodes, #entries
            SIZEOF_NBYTES + // max record length
            Bytes.SIZEOF_BYTE + // useChecksum
            SIZEOF_ADDR * 5 + // leaves, nodes, root, ext metadata, bloomFilter
            Bytes.SIZEOF_DOUBLE + // errorRate
            Bytes.SIZEOF_LONG + // file size
            Bytes.SIZEOF_UUID + // index UUID. 
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
     * 
     * @see #indexUUID
     */
    final public UUID segmentUUID;

    /**
     * Branching factor for the index segment.
     */
    final public int branchingFactor;
    
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
     */
    final public int maxNodeOrLeafLength;
    
    /**
     * When true, the checksum was computed and stored for the nodes and leaves
     * in the file and will be verified on de-serialization.
     */
    final public boolean useChecksum;
    
    /**
     * The {@link Addr address} of the contiguous region containing the
     * serialized leaves in the file.
     * <p>
     * Note: The offset component of this address must be equal to {@link #SIZE}
     * since the leaves are written immediately after the
     * {@link IndexSegmentMetadata} record.
     */
    final public long addrLeaves;
    
    /**
     * The {@link Addr address} of the contiguous region containing the
     * serialized nodes in the file or <code>0L</code> iff there are no nodes
     * in the file.
     */
    final public long addrNodes;
    
    /**
     * Address of the root node or leaf in the file.
     * 
     * @see Addr
     */
    final public long addrRoot;

    /**
     * The address of the {@link IndexSegmentExtensionMetadata} record.
     */
    final public long addrExtensionMetadata;
    
    /**
     * Address of the optional bloom filter and 0L iff no bloom filter
     * was constructed.
     * 
     * @see Addr
     */
    final public long addrBloom;
    
    /**
     * The target error rate for the optional bloom filter and 0.0 iff
     * the bloom filter was not constructed.
     */
    final public double errorRate;
    
    /**
     * Length of the file in bytes.
     */
    final public long length;
    
    /**
     * The unique identifier for the index whose data is on this
     * {@link IndexSegment}.
     * <p>
     * All {@link AbstractBTree}s having data for the same index will have the
     * same {@link #indexUUID}. A partitioned index is comprised of mutable
     * {@link BTree}s and historical read-only {@link IndexSegment}s, all of
     * which will have the same {@link #indexUUID} if they have data for the
     * same scale-out index.
     * 
     * @see #segmentUUID
     */
    final public UUID indexUUID;
    
    /**
     * Timestamp when the {@link IndexSegment} was generated.
     */
    final public long timestamp;

    /**
     * Reads the metadata record for the {@link IndexSegment} from the current
     * position in the file.
     * 
     * @param raf
     *            The file.
     * 
     * @throws IOException
     */
    public IndexSegmentMetadata(RandomAccessFile raf) throws IOException {

//        raf.seek(0);
        
        final int magic = raf.readInt();

        if (magic != MAGIC) {
            throw new IOException("Bad magic: actual=" + magic
                    + ", expected=" + MAGIC);
        }

        // @todo add assertions here parallel to those in the other ctor.

        final int version = raf.readInt();
        
        if( version != VERSION0 ) {
            
            throw new IOException("unknown version="+version);
            
        }

        final long timestamp0 = raf.readLong();
        
        segmentUUID = new UUID(raf.readLong()/*MSB*/, raf.readLong()/*LSB*/);
        
        branchingFactor = raf.readInt();

        height = raf.readInt();
        
        nleaves = raf.readInt();
        
        nnodes = raf.readInt();
        
        nentries = raf.readInt();

        maxNodeOrLeafLength = raf.readInt();
        
        useChecksum = raf.readBoolean();
                
        addrLeaves = raf.readLong();

        addrNodes = raf.readLong();
        
        addrRoot = raf.readLong();

        addrExtensionMetadata = raf.readLong();

        addrBloom = raf.readLong();
        
        errorRate = raf.readDouble();
        
        length = raf.readLong();
        
        if (length != raf.length()) {
            throw new IOException("Length differs: actual="
                    + raf.length() + ", expected=" + length);
        }
        
        indexUUID = new UUID(raf.readLong()/*MSB*/, raf.readLong()/*LSB*/);
        
        raf.skipBytes(SIZEOF_UNUSED);
        
        final long timestamp1 = raf.readLong();
        
        if(timestamp0 != timestamp1) {
            
            throw new RuntimeException("Timestamps do not agree - file is not useable.");
            
        }
        
        this.timestamp = timestamp0;
        
    }

    /**
     * Create a new metadata record in preparation for writing it on a file
     * containing a newly constructed {@link IndexSegment}.
     * 
     * @todo javadoc.
     */
    public IndexSegmentMetadata(int branchingFactor, int height,
            boolean useChecksum, int nleaves, int nnodes, int nentries,
            int maxNodeOrLeafLength, long addrLeaves, long addrNodes,
            long addrRoot, long addrExtensionMetadata, long addrBloom,
            double errorRate, long length, UUID indexUUID, UUID segmentUUID,
            long timestamp) {
        
        assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
        
        assert height >= 0;
        
        assert nleaves > 0; // always at least one leaf.
        
        assert nnodes >= 0; // may be just a root leaf.
        
        // index may not be empty.
        assert nentries > 0;
        // #entries must fit within the tree height.
        assert nentries <= Math.pow(branchingFactor,height+1);
        
        assert maxNodeOrLeafLength > 0;
        
        assert addrLeaves != 0L;
        assert Addr.getOffset(addrLeaves)==SIZE;
        assert Addr.getByteCount(addrLeaves) > 0;

        if(nnodes == 0) {
            // the root is a leaf.
            assert addrNodes == 0L;
            assert Addr.getOffset(addrLeaves) < length;
            assert Addr.getOffset(addrRoot) >= Addr.getOffset(addrLeaves);
            assert Addr.getOffset(addrRoot) < length;
        } else {
            // the root is a node.
            assert Addr.getOffset(addrNodes) > Addr.getOffset(addrLeaves);
            assert Addr.getOffset(addrNodes) < length;
            assert Addr.getOffset(addrRoot) >= Addr.getOffset(addrNodes);
            assert Addr.getOffset(addrRoot) < length;
        }

        if( addrBloom == 0L ) assert errorRate == 0.;
        
        if( errorRate != 0.) assert addrBloom != 0L;
        
        assert timestamp != 0L;
        
        assert segmentUUID != null;
        
        assert indexUUID != null;
        
        this.segmentUUID = segmentUUID;

        this.branchingFactor = branchingFactor;

        this.height = height;

        this.nleaves = nleaves;
        
        this.nnodes = nnodes;
        
        this.nentries = nentries;

        this.maxNodeOrLeafLength = maxNodeOrLeafLength;
        
        this.useChecksum = useChecksum;
        
        this.addrLeaves = addrLeaves;
        
        this.addrNodes = addrNodes;
        
        this.addrRoot = addrRoot;

        this.addrExtensionMetadata = addrExtensionMetadata;

        this.addrBloom = addrBloom;
        
        this.errorRate = errorRate;
        
        this.length = length;
        
        this.indexUUID = indexUUID;
        
        this.timestamp = timestamp;
        
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
        
        raf.writeInt(MAGIC);

        raf.writeInt(VERSION0);

        raf.writeLong(timestamp);
        
        raf.writeLong(segmentUUID.getMostSignificantBits());

        raf.writeLong(segmentUUID.getLeastSignificantBits());

        raf.writeInt(branchingFactor);
                        
        raf.writeInt(height);
        
        raf.writeInt(nleaves);

        raf.writeInt(nnodes);
        
        raf.writeInt(nentries);

        raf.writeInt(maxNodeOrLeafLength);
        
        raf.writeBoolean(useChecksum);
        
        raf.writeLong(addrLeaves);
        
        raf.writeLong(addrNodes);

        raf.writeLong(addrRoot);

        raf.writeLong(addrExtensionMetadata);
        
        raf.writeLong(addrBloom);
        
        raf.writeDouble(errorRate);
        
        raf.writeLong(length);
        
        raf.writeLong(indexUUID.getMostSignificantBits());

        raf.writeLong(indexUUID.getLeastSignificantBits());

        raf.skipBytes(SIZEOF_UNUSED);
        
        raf.writeLong(timestamp);
        
    }
    
    /**
     * A human readable representation of the metadata record.
     */
    public String toString() {
 
        StringBuilder sb = new StringBuilder();
        
        sb.append("magic="+Integer.toHexString(MAGIC));
        sb.append(", segmentUUID="+segmentUUID);
        sb.append(", branchingFactor="+branchingFactor);
        sb.append(", height=" + height);
        sb.append(", nleaves=" + nleaves);
        sb.append(", nnodes=" + nnodes);
        sb.append(", nentries=" + nentries);
        sb.append(", maxNodeOrLeafLength=" + maxNodeOrLeafLength);
        sb.append(", useChecksum=" + useChecksum);
        sb.append(", addrLeaves=" + Addr.toString(addrLeaves));
        sb.append(", addrNodes=" + Addr.toString(addrNodes));
        sb.append(", addrRoot=" + Addr.toString(addrRoot));
        sb.append(", addrExtensionMetadata=" + Addr.toString(addrExtensionMetadata));
        sb.append(", addrBloom=" + Addr.toString(addrBloom));
        sb.append(", errorRate=" + errorRate);
        sb.append(", length=" + length);
        sb.append(", indexUUID="+indexUUID);
        sb.append(", timestamp=" + new Date(timestamp));

        return sb.toString();
    }

}
