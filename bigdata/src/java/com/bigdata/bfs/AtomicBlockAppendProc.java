package com.bigdata.bfs;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILinearList;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.btree.view.FusedView;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;

/**
 * Atomic append of a single block to a file version.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AtomicBlockAppendProc implements ISimpleIndexProcedure,
        Externalizable {

    private static final long serialVersionUID = 1441331704737671258L;

    protected static transient Logger log = Logger
            .getLogger(AtomicBlockAppendProc.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static transient boolean INFO = log.getEffectiveLevel()
            .toInt() <= Level.INFO.toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static transient boolean DEBUG = log.getEffectiveLevel()
            .toInt() <= Level.DEBUG.toInt();

    private String id;
    private int version;
    private int off;
    private int len;
    private byte[] b;
    
    public final boolean isReadOnly() {
        
        return false;
        
    }
    
    /**
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The file version.
     * @param b
     *            The buffer containing the data to be written.
     * @param off
     *            The offset in the buffer of the first byte to be written.
     * @param len
     *            The #of bytes to be written.
     */
    public AtomicBlockAppendProc(BigdataFileSystem repo, String id,
            int version, byte[] b, int off, int len) {

        assert id != null && id.length() > 0;
        assert version >= 0;
        assert b != null;
        assert off >= 0 : "off="+off;
        assert len >= 0 && off + len <= b.length;
        assert len <= repo.getBlockSize(): "len="+len+" exceeds blockSize="+repo.getBlockSize();

        this.id = id;
        this.version = version;
        this.off = off;
        this.len = len;
        this.b = b;

    }
    
    /**
     * This procedure runs on the unisolated index. The block identifier is
     * computed as a one up long integer for that file version using locally
     * available state. The raw data for the block is written directly onto
     * the {@link Journal} and an index entry is added for the file,
     * version, and block whose value is the address of the block's data on
     * the {@link Journal}.
     * <p>
     * Note: The caller MUST have correctly identified the data service on
     * which the tail of the file exists (or on which the head of the file
     * will be written).
     * <p>
     * The block identifier is computed by reading and decoding the key for
     * the last block written for this file version (if any). Special cases
     * exist when the file version spans more than one index partition, when
     * the block would be the first block (in key order) for the index
     * partition, and when the block would be the last block (in key order)
     * for the index partition.
     * 
     * @return <code>true</code> iff the block was overwritten.
     */
    public Object apply(IIndex ndx) {

        // tunnel through to the backing journal.
        final AbstractJournal journal = (AbstractJournal)((AbstractBTree)ndx).getStore();
        
        // obtain the thread-local key builder for that journal.
        final IKeyBuilder keyBuilder = ndx.getIndexMetadata().getKeyBuilder();
        
        /*
         * The next block identifier to be assigned.
         */
        final long block = getNextBlockIdentifierInFileVersion(ndx, keyBuilder);

        if (log.isInfoEnabled())
            log.info("Will write " + len + " bytes on id=" + id + ", version="
                    + version + ", block#=" + block);
            
        {

            /*
             * write the block on the journal obtaining the address at which
             * it was written - use 0L for the address of an empty block.
             */
            final long addr = len == 0 ? 0L : journal.write(ByteBuffer
                    .wrap(b, off, len));

            // form the key for the index entry for this block.
            final byte[] key = keyBuilder.reset().appendText(id,
                    true/* unicode */, false/* successor */).append(
                    version).append(block).getKey();

            // record the address of the block in the index.
            {

                final DataOutputBuffer out = new DataOutputBuffer(
                        Bytes.SIZEOF_LONG);

                // encode the value for the entry.
                out.reset().putLong(addr);

                final byte[] val = out.toByteArray();

                // insert the entry into the index.
                ndx.insert(key, val);

            }

            if (log.isInfoEnabled())
                log.info("Wrote " + len + " bytes : id=" + id + ", version="
                        + version + ", block#=" + block + " @ addr"
                        + journal.toString(addr));

        }

        // the block identifier.
        return block;

    }

    /**
     * Find the key for the last block written for this file version. We do this
     * by forming a probe key from the file, version, and the maximum allowed
     * block identifier. This is guarenteed to be after any existing block for
     * that file and version.
     * <p>
     * Note: This implementation uses an {@link IRangeQuery#REVERSE} iterator to
     * locate the last block in the file and is capable of scale-out.
     * 
     * @todo This implies that the leftSeparator for the index partition MUST
     *       NOT split the blocks for a file unless there is at least one block
     *       in the index partition. In practice this guarentee is easy to
     *       maintain. By default we choose to split an index partition on a
     *       file boundary. If that would result in an uneven split (or an empty
     *       split in the case of very large files) then we choose a split point
     *       that lies within the file's data - leaving at least one block for
     *       the file (probably many) in both partitions created by the split.
     * 
     * @param ndx
     * @param keyBuilder
     * @return
     */
    protected long getNextBlockIdentifierInFileVersion2(IIndex ndx,
            IKeyBuilder keyBuilder) {
        
        final byte[] fromKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(
                version).append(0/*first valid block*/).getKey();
        
        final byte[] toKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(
                version).append(Long.MAX_VALUE/*max block*/).getKey();
    
        ITupleIterator itr = ndx
                .rangeIterator(fromKey, toKey, 1/* capacity */,
                        IRangeQuery.KEYS | IRangeQuery.REVERSE, null/*filter*/);
        
        if(!itr.hasNext()) {
            
            // There are no blocks for this file version.
            return 0L;
            
        }
        
        final byte[] key = itr.next().getKey();
        
        return getNextBlockFromPriorKey(keyBuilder, key);
        
    }
    
    /**
     * Find the key for the last block written for this file version. We do this
     * by forming a probe key from the file, version, and the maximum allowed
     * block identifier. This is guarenteed to be after any existing block for
     * that file and version.
     * <p>
     * Note: This implementation uses the {@link ILinearList} API to locate the
     * last block in the file and is NOT capable of scale-out since that API is
     * NOT available for an index partition view (a {@link FusedView}).
     * 
     * @todo This implies that the leftSeparator for the index partition MUST
     *       NOT split the blocks for a file unless there is at least one block
     *       in the index partition. In practice this guarentee is easy to
     *       maintain. By default we choose to split an index partition on a
     *       file boundary. If that would result in an uneven split (or an empty
     *       split in the case of very large files) then we choose a split point
     *       that lies within the file's data - leaving at least one block for
     *       the file (probably many) in both partitions created by the split.
     */
    protected long getNextBlockIdentifierInFileVersion(IIndex ndx,
            IKeyBuilder keyBuilder) {
        
        final byte[] toKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(
                version).append(Long.MAX_VALUE).getKey();

        // Note: uses the ILinearList API.
        final ILinearList tmp = (ILinearList) ndx;
        
        /*
         * Index of the first key after this file version.
         * 
         * Note: This will always be an insertion point (a negative
         * value) since the toKey only encodes the successor of the file
         * identifier.
         * 
         * We convert the insertion point to an index.
         * 
         * If the index is zero (0) then there are no blocks for this
         * file and the file will be the first file in the index order
         * on this index partition (there may or may not be other files
         * already on the index partition).
         * 
         * Else fetch the key at that index. If that key encodes the
         * same id as this file then we are appending to a file with
         * existing block(s) and we decode the block identifier from the
         * key. Otherwise this will be the first block written for that
         * file.
         */
        int toIndex = tmp.indexOf(toKey);

        assert toIndex < 0 : "Expecting insertion point: id=" + id
                + ", version=" + version + ", toIndex=" + toIndex;

        if (log.isDebugEnabled())
            log.debug("insertionPoint=" + toIndex);

        toIndex = -(toIndex + 1); // convert to an index.

        // #of entries in the index.
        final int entryCount = ((AbstractBTree) ndx).getEntryCount();

        if (log.isDebugEnabled())
            log.debug("toIndex=" + toIndex + ", entryCount=" + entryCount);

        final long block;
        
        if (toIndex == 0) {

            /*
             * Insertion point is before all other entries in the index.
             * 
             * Note: In this case we need to examine the leftSeparator
             * key for the index partition. If that key is for the same
             * file version then we use the successor of the block
             * identifier found in that key.
             * 
             * Note: when it is not for the same file version it MAY be
             * that the leftSeparator does not include the block
             * identifier - the block identifier is only required in the
             * leftSeparator when the a file version spans both the
             * prior index partition and this index partition.
             */
            
            if(log.isDebugEnabled()) 
                log.debug("Insertion point is before all entries in the index partition: id="
                            + id + ", version=" + version);
            
            final byte[] leftSeparator = ((BTree) ndx)
                    .getIndexMetadata().getPartitionMetadata()
                    .getLeftSeparatorKey();

            block = getNextBlockFromPriorKey(keyBuilder, leftSeparator);
            
        } else {
            
            if (toIndex == entryCount) {

                /*
                 * Insertion point is after all entries in the index.
                 * 
                 * Note: In this case we consider the prior key in the
                 * index partition. If that key is for the same file
                 * version then we use the successor of the block
                 * identifier found in that key.
                 */

                if (log.isDebugEnabled())
                    log.debug("Insertion point is after all entries in the index partition: id="
                                    + id + ", version=" + version);

            } else {

                /*
                 * Insertion point is at the toKey.
                 * 
                 * Note: Since the probe key is beyond the last block
                 * for the file version we adjust the toIndex so that we
                 * consider the prior key.
                 */

                if (log.isDebugEnabled())
                    log.debug("Insertion point is at the toKey: id=" + id
                            + ", version=" + version);

            }

            /*
             * Adjust to consider the key before the insertion point.
             */

            toIndex--;
            
            /*
             * Look at the key at the computed index. If it is a key for
             * this file version then we use the successor of the given
             * block identifier. Otherwise we are writing a new file
             * version and the block identifier will be zero (0).
             */
            
            if (log.isDebugEnabled())
                log.debug("adjusted toIndex="+toIndex+", entryCount="+entryCount);
            
            // the key at that index.
            final byte[] key = tmp.keyAt(toIndex);

            assert key != null : "Expecting entry: id=" + id
                    + ", version=" + version + ", toIndex=" + toIndex;

            block = getNextBlockFromPriorKey(keyBuilder, key);
            
        }

        return block;
        
    }
    
    /**
     * Decode the block identifier in the key and return the block
     * identifier plus one, which is the block identifier to be used for the
     * atomic append operation. If the key does NOT encode the same file +
     * version then no blocks exist for that file version and the method
     * returns zero (0L) as the block identifer to be used.
     * 
     * @param keyBuilder
     *            The key builder.
     * @param key
     *            The key - either from the index partition or in some cases
     *            from the leftSeparator of the index partition metadata.
     *            <p>
     *            Note that the leftSeparator MAY be an empty byte[] (e.g.,
     *            for the 1st index partition in the key order) and MIGHT
     *            NOT include the block identifier (the block identifier is
     *            only included when it is necessary to split a file across
     *            index partitions). When the block identifier is omitted
     *            from the key and the key encodes the same file and version
     *            we therefore use zero (0L) as the next block identifier
     *            since we will be appending the first block to the file
     *            version.
     * 
     * @return The block identifier that will be used by the atomic append
     *         operation.
     */
    protected long getNextBlockFromPriorKey(IKeyBuilder keyBuilder,
            byte[] key) {

        // encode just the file id and the version.
        final byte[] prefix = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .getKey();

        if (DEBUG)
            log.debug("Comparing\nkey   :" + Arrays.toString(key)
                    + "\nprefix:" + Arrays.toString(prefix));

        /*
         * Test the encoded file id and version against the encoded file id
         * and version in the recovered key. If they compare equals (for the
         * length of the key that we just built) then they encode the same
         * file id and version.
         * 
         * (I.e., if true, then the key is from a block entry for this
         * version of this file).
         */

        if (key.length >= prefix.length) {

            final int cmp = BytesUtil.compareBytesWithLenAndOffset(0,
                    prefix.length, prefix, 0, prefix.length, key);

            if(DEBUG)
            log.debug("Comparing " + prefix.length + " byte prefix with "
                    + key.length + " byte key: cmp=" + cmp);

            if (cmp == 0) {

                /*
                 * The key at the computed toIndex is the same file version.
                 */
                if (prefix.length + Bytes.SIZEOF_LONG == key.length) {
                    
                    /*
                     * The given key includes a block identifier so we
                     * extract it.
                     * 
                     * Note: When the given key is a leftSeparator for an
                     * index partition AND the file version is not split
                     * across the index partition then the block identifer
                     * MAY be omitted from the leftSeparator. In this case
                     * the block identifier will be zero since there are no
                     * blocks yet for that file version.
                     */

                    // last block identifier assigned for this file + 1.
                    final long block = KeyBuilder.decodeLong(key,
                            key.length - Bytes.SIZEOF_LONG) + 1;

                    if (block > BigdataFileSystem.MAX_BLOCK) {

                        throw new RuntimeException(
                                "File version has maximum #of blocks: id="
                                        + id + ", version=" + version);

                    }

                    if(INFO)
                    log.info("Appending to existing file version: id=" + id
                            + ", version=" + version + ", block=" + block);

                    return block;

                } else {
                    
                    /*
                     * This case arises when the leftSeparator encodes the
                     * file version but does not include a block identifier.
                     */
                    
                    if(INFO)
                    log.info("Key is for same file version but does not contain block identifier.");
                    
                }
                
            } else {
                
                /*
                 * Since the key does not compare as equal for the full
                 * length of the prefix it can not encode the same file
                 * version.
                 */
                
                if(DEBUG)
                log.debug("Key does not compare as equal for length of prefix.");
                
            }

        } else {
            
            /*
             * Since the key is shorter than the prefix it can not be for
             * the same file version.
             */
            
            log.debug("Key is shorter than prefix.");
            
        }

        /*
         * The key at computed toIndex is a different file version so we are
         * starting a new file version at block := 0.
         */

        if(INFO)
        log.info("Appending to new file version: id=" + id + ", version="
                + version + ", block=" + 0L);

        return 0L;

    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        id = in.readUTF();

        version = in.readInt();

        off = 0; // Note: offset always zero when de-serialized.

        len = in.readInt();

        b = new byte[len];

        in.readFully(b);

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeUTF(id);

        out.writeInt(version);

        /*
         * Note: offset not written when serialized and always zero when
         * de-serialized.
         */
        
        out.writeInt(len); /* length */
        
        out.write(b, off, len); /* data */
        
    }
    
}