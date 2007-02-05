package com.bigdata.objndx;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Date;

import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.Bytes;

/**
 * The metadata record for an {@link IndexSegment}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo consider recording the min/max key or just making it easy to determine
 *       that for an {@link IndexSegment}. This has do to with both correct
 *       rejection of queries directed to the wrong index segment and managing
 *       the metadata for a distributed index.
 * 
 * @todo add a uuid for each index segment and a uuid for the index to which the
 *       segments belong? examine the format of the uuid. can we use part of it
 *       as the unique basis for one up identifiers within a parition?
 * 
 * FIXME We need a general mechanism for persisting metadata including the
 * valSer, record compressor, and user-defined objects for indices. These data
 * can go into a series of extensible metadata records located at the end of the
 * file. The bloom filter itself could be an example of such a metadata record.
 * Such metadata should survive conversions from a btree to an index segment,
 * mergers of index segments or btrees, and conversion from an index segment to
 * a btree.
 * 
 * FIXME introduce two timestamps in the metadata record. the record is valid
 * iff both timestamps agree and are non-zero.
 */
public class IndexSegmentMetadata {
    
    /**
     * Magic value written at the start of the metadata record.
     */
    static transient final public int MAGIC = 0x87ab34f5;
    
    /**
     * Version 0 of the serialization format.
     */
    static transient final public int VERSION0 = 0x0;
    
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
     * When true, the checksum was computed and stored for the nodes and leaves
     * in the file and will be verified on de-serialization.
     */
    final public boolean useChecksum;
    
    /**
     * When true, a {@link RecordCompressor} was used to write the nodes and
     * leaves of the {@link IndexSegment}.
     * 
     * @todo modify to specify the implementation of a record compressor
     *       interface.
     */
    final public boolean useRecordCompressor;
    
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
     * The target error rate for the optional bloom filter and 0.0 iff
     * the bloom filter was not constructed.
     */
    final public double errorRate;
    
    /**
     * Address of the optional bloom filter and 0L iff no bloom filter
     * was constructed.
     * 
     * @see Addr
     */
    final public long addrBloom;
    
    /**
     * Length of the file in bytes.
     */
    final public long length;
    
    /**
     * Timestamp when the {@link IndexSegment} was generated.
     */
    final public long timestamp;
    
    /**
     * @todo Name of the index?  or uuid? or drop?
     */
    final public String name;

    /**
     * The #of bytes in the metadata record.
     * 
     * @todo This is oversized in order to allow some slop for future entries
     *       and in order to permit the variable length index name to be
     *       recorded in the index segment file.  The size needs to be reviewed
     *       once the design is crisper.
     */
    public static final int SIZE = Bytes.kilobyte32 * 4;
    
    public static final int MAX_NAME_LENGTH = Bytes.kilobyte32 * 2;

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
        
        branchingFactor = raf.readInt();

        height = raf.readInt();
        
        useChecksum = raf.readBoolean();
        
        useRecordCompressor = raf.readBoolean();
        
        nleaves = raf.readInt();
        
        nnodes = raf.readInt();
        
        nentries = raf.readInt();

        maxNodeOrLeafLength = raf.readInt();
        
        addrLeaves = raf.readLong();

        addrNodes = raf.readLong();
        
        addrRoot = raf.readLong();

        errorRate = raf.readDouble();
        
        addrBloom = raf.readLong();
        
        length = raf.readLong();
        
        if (length != raf.length()) {
            throw new IOException("Length differs: actual="
                    + raf.length() + ", expected=" + length);
        }
        
        timestamp = raf.readLong();

        name = raf.readUTF();
        
        assert name.length() <= MAX_NAME_LENGTH;
        
    }

    /**
     * Create a new metadata record in preparation for writing it on a file
     * containing a newly constructed {@link IndexSegment}.
     * 
     * @todo javadoc.
     */
    public IndexSegmentMetadata(int branchingFactor, int height,
            boolean useChecksum, boolean useRecordCompressor, int nleaves,
            int nnodes, int nentries, int maxNodeOrLeafLength,
            long addrLeaves, long addrNodes, long addrRoot,
            double errorRate, long addrBloom, long length, long timestamp,
            String name) {
        
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
        
        assert name != null;
        
        assert name.length() <= MAX_NAME_LENGTH;

        this.branchingFactor = branchingFactor;

        this.height = height;

        this.useChecksum = useChecksum;

        this.useRecordCompressor = useRecordCompressor;
        
        this.nleaves = nleaves;
        
        this.nnodes = nnodes;
        
        this.nentries = nentries;

        this.maxNodeOrLeafLength = maxNodeOrLeafLength;
        
        this.addrLeaves = addrLeaves;
        
        this.addrNodes = addrNodes;
        
        this.addrRoot = addrRoot;

        this.errorRate = errorRate;
        
        this.addrBloom = addrBloom;
        
        this.length = length;
        
        this.timestamp = timestamp;

        this.name = name;
        
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
        
        raf.writeInt(branchingFactor);
                        
        raf.writeInt(height);
        
        raf.writeBoolean(useChecksum);
                
        raf.writeBoolean(useRecordCompressor);
        
        raf.writeInt(nleaves);

        raf.writeInt(nnodes);
        
        raf.writeInt(nentries);

        raf.writeInt(maxNodeOrLeafLength);
        
        raf.writeLong(addrLeaves);
        
        raf.writeLong(addrNodes);

        raf.writeLong(addrRoot);

        raf.writeDouble(errorRate);
        
        raf.writeLong(addrBloom);
        
        raf.writeLong(length);
        
        raf.writeLong(timestamp);
        
        raf.writeUTF(name);
        
    }
    
    /**
     * A human readable representation of the metadata record.
     */
    public String toString() {
 
        StringBuilder sb = new StringBuilder();
        
        sb.append("magic="+Integer.toHexString(MAGIC));
        sb.append(", branchingFactor="+branchingFactor);
        sb.append(", height=" + height);
        sb.append(", useChecksum=" + useChecksum);
        sb.append(", useRecordCompressor=" + useRecordCompressor);
        sb.append(", nleaves=" + nleaves);
        sb.append(", nnodes=" + nnodes);
        sb.append(", nentries=" + nentries);
        sb.append(", maxNodeOrLeafLength=" + maxNodeOrLeafLength);
        sb.append(", addrLeaves=" + addrLeaves);
        sb.append(", addrNodes=" + addrNodes);
        sb.append(", addrRoot=" + Addr.toString(addrRoot));
        sb.append(", errorRate=" + errorRate);
        sb.append(", addrBloom=" + Addr.toString(addrBloom));
        sb.append(", length=" + length);
        sb.append(", timestamp=" + new Date(timestamp));
        sb.append(", name="+name);

        return sb.toString();
    }

}
