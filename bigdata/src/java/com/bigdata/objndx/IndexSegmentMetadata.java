package com.bigdata.objndx;

import java.io.IOException;
import java.io.RandomAccessFile;

import com.bigdata.journal.Bytes;

/**
 * The metadata record for an {@link IndexSegment}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo consider recording the min/max key or just making it easy to determine
 *       that for an {@link IndexSegment}.  This has do to with both correct
 *       rejection of queries directed to the wrong index segment and managing
 *       the metadata for a distributed index.
 * 
 * @todo add a uuid for each index segment and a uuid for the index to which the
 *       segments belong?
 * 
 * @todo We need a general mechanism for persisting metadata including NEGINF,
 *       Comparator, keySer, and valSer for indices.  This can probably be done
 *       using a fat metadata record and standard Java serialization for these
 *       things rather than trying to bootstrap extSer for this purpose.
 */
public class IndexSegmentMetadata {
    
    /**
     * Magic value written at the start of the metadata record.
     */
    static final public int MAGIC = 0x87ab34f5;
    
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
     * The key type used in the index.
     */
    final public ArrayType keyType;

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
     * The offset to the start of the serialized leaves in the file.
     * 
     * Note: This should be equal to {@link #SIZE} since the leaves are
     * written immediately after the {@link IndexSegmentMetadata} record.
     */
    final public long offsetLeaves;
    
    /**
     * The offset to the start of the serialized nodes in the file or
     * <code>0L</code> iff there are no nodes in the file.
     */
    final public long offsetNodes;
    
    /**
     * Address of the root node or leaf in the file.
     * 
     * @see Addr
     */
    final public long addrRoot;
    
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
        
        branchingFactor = raf.readInt();

        height = raf.readInt();
        
        keyType = ArrayType.parseInt(raf.readInt());
        
        nleaves = raf.readInt();
        
        nnodes = raf.readInt();
        
        nentries = raf.readInt();

        maxNodeOrLeafLength = raf.readInt();
        
        offsetLeaves = raf.readLong();

        offsetNodes = raf.readLong();
        
        addrRoot = raf.readLong();

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
     * @param branchingFactor
     * @param height
     * @param keyType
     * @param nleaves
     * @param nnodes
     * @param nentries
     * @param maxNodeOrLeafLength
     * @param offsetLeaves
     * @param offsetNodes
     * @param addrRoot
     * @param length
     * @param timestamp
     * @param name
     * 
     * @todo javadoc.
     */
    public IndexSegmentMetadata(int branchingFactor, int height,
            ArrayType keyType, int nleaves, int nnodes, int nentries,
            int maxNodeOrLeafLength, long offsetLeaves, long offsetNodes,
            long addrRoot, long length, long timestamp, String name) {
        
        assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
        
        assert height >= 0;
        
        assert keyType != null;
        
        assert nleaves > 0; // always at least one leaf.
        
        assert nnodes >= 0; // may be just a root leaf.
        
        // index may not be empty.
        assert nentries > 0;
        // #entries must fit within the tree height.
        assert nentries <= Math.pow(branchingFactor,height+1);
        
        assert maxNodeOrLeafLength > 0;
        
        assert offsetLeaves > 0;

        if(nnodes == 0) {
            // the root is a leaf.
            assert offsetNodes == 0L;
            assert offsetLeaves < length;
            assert Addr.getOffset(addrRoot) >= offsetLeaves;
            assert Addr.getOffset(addrRoot) < length;
        } else {
            // the root is a node.
            assert offsetNodes > offsetLeaves;
            assert offsetNodes < length;
            assert Addr.getOffset(addrRoot) >= offsetNodes;
            assert Addr.getOffset(addrRoot) < length;
        }
        
        assert timestamp != 0L;
        
        assert name != null;
        
        assert name.length() <= MAX_NAME_LENGTH;
        
        this.branchingFactor = branchingFactor;

        this.height = height;
        
        this.keyType = keyType;

        this.nleaves = nleaves;
        
        this.nnodes = nnodes;
        
        this.nentries = nentries;

        this.maxNodeOrLeafLength = maxNodeOrLeafLength;
        
        this.offsetLeaves = offsetLeaves;
        
        this.offsetNodes = offsetNodes;
        
        this.addrRoot = addrRoot;

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
        
        raf.writeInt(branchingFactor);
                        
        raf.writeInt(height);
        
        raf.writeInt(keyType.intValue());
        
        raf.writeInt(nleaves);

        raf.writeInt(nnodes);
        
        raf.writeInt(nentries);

        raf.writeInt(maxNodeOrLeafLength);
        
        raf.writeLong(offsetLeaves);
        
        raf.writeLong(offsetNodes);

        raf.writeLong(addrRoot);

        raf.writeLong(length);
        
        raf.writeLong(timestamp);
        
        raf.writeUTF(name);
        
    }

}