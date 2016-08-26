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
 * Created on Nov 5, 2006
 */
package com.bigdata.htree;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import com.bigdata.BigdataStatics;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.data.AbstractReadOnlyNodeData;
import com.bigdata.btree.data.DefaultLeafCoder;
import com.bigdata.btree.data.IAbstractNodeData;
import com.bigdata.btree.data.IAbstractNodeDataCoder;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.data.INodeData;
import com.bigdata.htree.data.DefaultDirectoryPageCoder;
import com.bigdata.htree.data.IDirectoryData;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.FixedByteArrayBuffer;
import com.bigdata.io.IDataRecord;
import com.bigdata.io.compression.IRecordCompressor;
import com.bigdata.io.compression.IRecordCompressorFactory;
import com.bigdata.io.compression.NOPRecordCompressor;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.util.Bytes;

/**
 * <p>
 * An instance of this class is used to serialize and de-serialize the
 * {@link INodeData}s and {@link ILeafData}s of an {@link AbstractBTree}. Leaf
 * and non-leaf records have different serialization formats, but their leading
 * bytes use the same format so that you can tell by inspection whether a buffer
 * contains a leaf or a non-leaf node. The header of the record uses a fixed
 * length format so that some fields can be tested without full
 * de-serialization, especially whether the record contains a leaf vs a node.
 * This fixed record also makes it possible to update some fields in the header
 * once the entire record has been serialized, including the checksum, the #of
 * bytes in the serialized record, and the prior/next addresses for leaves.
 * </p>
 * <p>
 * The methods defined by this class all work with {@link ByteBuffer}s. On read,
 * the buffer must be positioned to the start of the data to be read. After a
 * read, the buffer will be positioned to the first byte after the data read. If
 * there is insufficient data available in the buffer then an
 * {@link BufferUnderflowException} will be thrown. On write, the data will be
 * written on an internal buffer whose size is automatically extended. The write
 * buffer is reused for each write and quickly achieves a maximum size for any
 * given {@link HTree}.
 * </p>
 * <p>
 * Note: while the {@link NodeSerializer} is NOT thread-safe for writers, it is
 * thread-safe for readers. This design mirrors the concurrency capabilities of
 * the {@link AbstractBTree}.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: NodeSerializer.java 3305 2010-07-27 15:08:28Z thompsonbry $
 * 
 * @see AbstractBTree
 * @see IndexMetadata
 * @see IAbstractNodeData
 * @see IAbstractNodeDataCoder
 */
public class NodeSerializer {

	/**
	 * An object that knows how to construct {@link DirectoryPage}s and
	 * {@link BucketPage}s from {@link IDirectoryData} and {@link ILeafData}
	 * objects.
	 */
	protected final INodeFactory nodeFactory;

    /**
     * When <code>true</code> the {@link NodeSerializer} instance will refuse to
     * {@link #encode(IAbstractNodeData)} nodes or leaves (this keeps us from
     * allocating the {@link #_writeBuffer}). Note that this MUST be
     * <code>false</code> for a transient B+Tree so we can convert its nodes and
     * leaves into coded data records as they are evicted from the write
     * retention queue.
     */
    private final boolean readOnly;

    /**
     * Used to code the nodes.
     */
    final IAbstractNodeDataCoder<IDirectoryData> nodeCoder;
    
    /**
     * Used to code the nodes.
     */
    final IAbstractNodeDataCoder<ILeafData> leafCoder;
    
    /**
     * Factory for record-level (de-)compression of nodes and leaves (optional).
     */
    final IRecordCompressorFactory<?> recordCompressorFactory;
    
    /**
     * An object that knows how to (de-)compress a node or leaf (optional).
     */
    private IRecordCompressor getRecordCompressor() {
        
        if (recordCompressorFactory == null) {
            
            return NOPRecordCompressor.INSTANCE;
            
        }

        if (!readOnly) {
            
            assert _writeCompressor != null;
            
            // Instance used for writes, which are single threaded.
            return _writeCompressor;
            
        }
        
        return recordCompressorFactory.getInstance();
        
    }

    /**
     * Used to serialize the nodes and leaves of the tree. This is pre-allocated
     * based on the estimated maximum size of a node or leaf and grows as
     * necessary when it overflows. The same buffer instance is used to
     * serialize all nodes and leaves of the tree.
     * <p>
     * Note: this buffer is discarded by {@link #close()} when the btree is
     * {@link AbstractBTree#close() closed} and then reallocated on demand.
     * <p>
     * Note: It is important that this field NOT be used for a read-only
     * {@link HTree} since only mutable {@link HTree}s are single threaded -
     * concurrent readers are allowed for read-only {@link HTree}s.
     * 
     * @see #allocWriteBuffer()
     * @see #close()
     */
    private DataOutputBuffer _writeBuffer;

    /**
     * Instance used for writes - this keeps a hard reference since writes are
     * single threaded.
     */
    private IRecordCompressor _writeCompressor;
    
    private final int initialBufferCapacity;

    /**
     * The default initial capacity multiplier for the (de-)serialization buffer.
     * The total initial buffer capacity is this value times the
     * effective branching factor, which is computed from the addressBits
     * constructor parameter.
     */
    public static final transient int DEFAULT_BUFFER_CAPACITY_PER_ENTRY = Bytes.kilobyte32 / 4;
    
    /**
     * Constructor is disallowed.
     */
    @SuppressWarnings("unused")
    private NodeSerializer() {

        throw new UnsupportedOperationException();

    }

    /**
     * Designated constructor.
     * 
     * @param nodeFactory
     *            An object that knows how to construct {@link INodeData}s and
     *            {@link ILeafData leaves}.
     * 
     * @param addressBits
     *            The #of address bits for target {@link HTree}.
     * 
     * @param initialBufferCapacity
     *            The initial capacity for internal buffer used to serialize
     *            nodes and leaves. The buffer will be resized as necessary
     *            until it is sufficient for the records being serialized for
     *            the {@link HTree}. When zero (0), a default is used. A
     *            non-zero value is worth specifying only when the actual buffer
     *            size is consistently less than the default for some
     *            {@link HTree}. See {@link #DEFAULT_BUFFER_CAPACITY_PER_ENTRY}
     * 
     * @param indexMetadata
     *            The {@link IndexMetadata} record for the index.
     * 
     * @param readOnly
     *            <code>true</code> IFF the caller is asserting that they WILL
     *            NOT attempt to serialize any nodes or leaves using this
     *            {@link NodeSerializer} instance.
     * 
     *            FIXME {@link IRecordCompressorFactory} should either be used
     *            here or moved into the {@link IRawStore} impl.
     * 
     * @todo the {@link IAddressManager} is not used any more. It was used by
     *       the {@link IAddressSerializer}.
     */
    public NodeSerializer(//
            final IAddressManager addressManager,
            final INodeFactory nodeFactory,//
            final int addressBits,//
            final int initialBufferCapacity, //
            final IndexMetadata indexMetadata,//
            final boolean readOnly,//
            final IRecordCompressorFactory<?> recordCompressorFactory
            ) {
        
        assert nodeFactory != null;

        assert initialBufferCapacity >= 0;

        assert indexMetadata != null;
        
        this.nodeFactory = nodeFactory;

        this.readOnly = readOnly;

		/*
		 * We are using a specialized coder for the directory pages since they
		 * do not share many characteristics with a BTree non-leaf node. In
		 * particular, there are no *keys* in a DirectoryPage and we do not
		 * current store spanned entry counts or other metadata. Right now, the
		 * only information in a DirectoryPage is the address map.
		 * 
		 * @todo parameter for the node/leaf coder impls or the NodeSerializer?
		 */
        this.nodeCoder = new DefaultDirectoryPageCoder();

        if (!indexMetadata.getTupleSerializer().getLeafKeysCoder().isDuplicateKeys()) {
            
            /*
             * This constraint *could* be relaxed, but the HTree API presumes
             * that we can have duplicate keys and this check verifies tha the
             * keys coder supports duplicate keys.
             */

            throw new IllegalArgumentException(
                    "The leaf keys coder for HTree should allow duplicate keys.");

        }
        
		/*
		 * Note: We are using the same leaf coder class as the BTree.
		 */
        this.leafCoder = new DefaultLeafCoder(indexMetadata
                .getTupleSerializer().getLeafKeysCoder(), indexMetadata
                .getTupleSerializer().getLeafValuesCoder());
        
        // MAY be null
        this.recordCompressorFactory = recordCompressorFactory;
        
        if (readOnly) {

            this.initialBufferCapacity = 0;
            
            this._writeBuffer = null;
            
        } else {
            
            if (initialBufferCapacity == 0) {
                
                // The effective branching factor.
                final int branchingFactor = (1 << addressBits);
                
                this.initialBufferCapacity = DEFAULT_BUFFER_CAPACITY_PER_ENTRY
                        * branchingFactor;

            } else {

                this.initialBufferCapacity = initialBufferCapacity;
                
            }

            // allocate initial write buffer.
            allocWriteBuffer();

        }

    }

    /**
     * Releases any buffers. They will be automatically reallocated if the
     * {@link NodeSerializer} is used again.
     */
    public void close() {

        _writeBuffer = null;
        
        _writeCompressor = null;

    }

    /**
     * Allocates {@link #_writeBuffer} with {@link #initialBufferCapacity}.
     * 
     * @throws UnsupportedOperationException
     *             if the {@link NodeSerializer} does not permit writes.
     */
    private void allocWriteBuffer() {

        if (readOnly) {

            throw new UnsupportedOperationException();
            
        }

        assert _writeBuffer == null;

        assert _writeCompressor == null;

        _writeBuffer = new DataOutputBuffer(initialBufferCapacity);
        
        _writeCompressor = recordCompressorFactory == null ? NOPRecordCompressor.INSTANCE
                : recordCompressorFactory.getInstance();

    }

    /**
     * Report the current write buffer capacity -or- the
     * {@link #initialBufferCapacity} if the write buffer is not allocated.
     */
    int getWriteBufferCapacity() {

        if (readOnly) {

            throw new UnsupportedOperationException();

        }

        final DataOutputBuffer tmp = _writeBuffer;
        
        if (tmp == null)
            return initialBufferCapacity;

        return tmp.capacity();

    }
    
    /**
     * Decode an {@link INodeData} or {@link ILeafData} record, wrapping the
     * underlying data record (thread-safe). The decision to decode as an
     * {@link INodeData} or {@link ILeafData} instance is made based on
     * inspection of the first byte byte in the supplied buffer, which codes for
     * a node, leaf, or linked-leaf.
     * 
     * @param buf
     *            The data record.
     * 
     * @return A {@link INodeData} or {@link ILeafData} instance for that data
     *         record.
     * 
     *         FIXME modify to accept {@link IDataRecord} rather than
     *         {@link ByteBuffer}.
     */
    public IAbstractNodeData decode(final ByteBuffer buf) {

        if (buf == null)
            throw new IllegalArgumentException();

        final boolean isNode = AbstractReadOnlyNodeData.isNode(buf
                .get(AbstractReadOnlyNodeData.O_TYPE));

        // FIXME should be done at the store level during decompress.
        final AbstractFixedByteArrayBuffer slice;
        if (!buf.hasArray()) {
            // backing array is not accessible, so copy into new byte[].
            final byte[] tmp = new byte[buf.remaining()];
            buf.get(tmp);
            slice = FixedByteArrayBuffer.wrap(tmp);
            if(BigdataStatics.debug)
                System.err.print("[RO]");
        } else {
            // backing array is accessible, so wrap as slice.
            slice = new FixedByteArrayBuffer(buf.array(), buf.arrayOffset(),
                    buf.capacity());
        }
        
        if(isNode) {

            return nodeCoder.decode(slice);
            
        }

        return leafCoder.decode(slice);

    }

    /**
     * Wrap an {@link INodeData} or {@link ILeafData} instance as a {@link DirectoryPage}
     * or a {@link BucketPage}. This DOES NOT set the parent of the new {@link DirectoryPage}
     * or {@link BucketPage}.
     * 
     * @param btree
     *            The owning B+Tree.
     * @param addr
     *            The address of the data record in the backing store.
     * @param data
     *            The data record.
     *            
     * @return The node or leaf.
     */
    public AbstractPage wrap(final AbstractHTree btree, final long addr,
            final IAbstractNodeData data) {

        //        assert btree != null;
        //        assert addr != 0L;

        if (data.isLeaf()) {

            // wrap data record as Leaf.
            return nodeFactory.allocLeaf(btree, addr, (ILeafData) data);

        } else {

            // wrap data record as Node.
            return nodeFactory.allocNode(btree, addr, (IDirectoryData) data);

        }

    }

    /**
     * Encode a node or leaf and return a coded instance of the persistent data
     * for that node or leaf backed by an exact fit byte[] (NOT thread-safe).
     * The operation writes on an internal buffer which is automatically
     * extended as required.
     * 
     * @param node
     *            The node or leaf.
     * 
     * @return The buffer containing the coded data record for the node or leaf
     *         in an exact fit byte[] owned by the coded node or leaf.
     */
    @SuppressWarnings("unchecked")
    public <T extends IAbstractNodeData> T encodeLive(final T node) {

        if (node == null)
            throw new IllegalArgumentException();

        if (node.isCoded()) {
        
            // already coded.
            throw new IllegalStateException();

        }
        
        if (_writeBuffer == null) {
         
            // re-allocate.
            allocWriteBuffer();
            
        } else {
        
            _writeBuffer.reset();
        
        }

        final T codedNode;
        if(node.isLeaf()) {

            codedNode = (T) leafCoder
                    .encodeLive((ILeafData) node, _writeBuffer);

        } else {

            codedNode = (T) nodeCoder
                    .encodeLive((IDirectoryData) node, _writeBuffer);

        }

        /*
         * Trim the backing byte[] buffer to an exact fit. All of the slice()s
         * based on that buffer will reference the new backing byte[]. trim()
         * returns the _old_ backing byte[], which we then wrap and use to
         * replace the write buffer. This allows the write buffer to grow until
         * it "fits" the coded data records, while preserving exact fit byte[]s
         * for each coded node or leaf whose slices for the coded keys and
         * values remain tied to the exact fit byte[].
         */
        _writeBuffer = new DataOutputBuffer(0/* len */, _writeBuffer.trim());

//        if (node.isLeaf() && leafCoder instanceof CanonicalHuffmanRabaCoder) {
//            // hack updates reference into the backing byte[]. 
//            ((CanonicalHuffmanRabaCoder.CodedRabaImpl) ((ILeafData) codedNode)
//                    .getValues()).trimmedSlice();
//        }
        
        // Return the coded node or leaf.
        return codedNode;

    }

    /**
     * Encode a node or leaf onto an internal buffer and return that buffer (NOT
     * thread-safe). This is a slight optimization of
     * {@link #encodeLive(IAbstractNodeData)} which is used when the written
     * node or leaf will not be reused, e.g., by the {@link IndexSegmentBuilder}
     * . The operation writes on an internal buffer which is automatically
     * extended as required.
     * 
     * @param node
     *            The node or leaf.
     * 
     * @return The buffer containing the coded data record for the node or leaf
     *         in a <em>shared buffer</em>. The contents of this buffer may be
     *         overwritten by the next node or leaf serialized the same instance
     *         of this class. The position will be zero and the limit will be
     *         the #of bytes in the coded representation.
     * 
     * @deprecated This method is no longer used since I refactored the
     *             {@link IndexSegmentBuilder} to optionally stuff the generated
     *             nodes and leaves into the cache. It still works but it might
     *             go away in the future.
     */
    public AbstractFixedByteArrayBuffer encode(final IAbstractNodeData node) {

        if (node == null)
            throw new IllegalArgumentException();

        if (node.isCoded()) {

            // already coded.
            throw new IllegalStateException();

        }

        if (_writeBuffer == null) {

            // re-allocate.
            allocWriteBuffer();

        } else {

            _writeBuffer.reset();

        }

        if (node.isLeaf()) {

            return leafCoder.encode((ILeafData) node, _writeBuffer);

        } else {

            return nodeCoder.encode((IDirectoryData) node, _writeBuffer);

        }

    }

    /**
     * Update the serialization of a leaf to set the prior and next leaf
     * references and change its serialization type from {@link #TYPE_LEAF} to
     * {@link #TYPE_LINKED_LEAF}.
     * <p>
     * Note: In order to use this method to write linked leaves on the store you
     * have to either write behind at a pre-determined address on the store or
     * settle for writing only the prior or the next leaf address, but not both.
     * It is up to the caller to perform these tricks. All this method does is
     * to touch up the serialized record.
     * <p>
     * Note: This method has NO side-effects on the <i>position</i> or
     * <i>limit</i> of the caller's {@link ByteBuffer}.
     * 
     * @param b
     *            The serialization leaf.
     * @param priorAddr
     *            The address of the previous leaf in key order, <code>0L</code>
     *            if it is known that there is no previous leaf, and
     *            <code>-1L</code> if either: (a) it is not known whether there
     *            is a previous leaf; or (b) it is known but the address of that
     *            leaf is not known to the caller.
     * @param nextAddr
     *            The address of the next leaf in key order, <code>0L</code> if
     *            it is known that there is no next leaf, and <code>-1L</code>
     *            if either: (a) it is not known whether there is a next leaf;
     *            or (b) it is known but the address of that leaf is not known
     *            to the caller.
     * 
     * @see IndexSegmentBuilder
     * @see DefaultLeafCoder
     */
    public void updateLeaf(final ByteBuffer b, final long priorAddr,
            final long nextAddr) {

        if (AbstractReadOnlyNodeData.isNode(b
                .get(AbstractReadOnlyNodeData.O_TYPE))) {

            throw new UnsupportedOperationException("Not a leaf.");

        }

        b.putLong(AbstractReadOnlyNodeData.O_PRIOR, priorAddr);

        b.putLong(AbstractReadOnlyNodeData.O_NEXT, nextAddr);

    }

}
