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

import it.unimi.dsi.util.BloomFilter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.io.SerializerUtil;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.rawstore.AbstractRawStore;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.MetadataService;

/**
 * A read-only store backed by a file containing a single {@link IndexSegment}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexSegmentStore extends AbstractRawStore implements IRawStore {

    /**
     * Logger.
     */
    protected static final Logger log = Logger
            .getLogger(IndexSegmentStore.class);

    /**
     * A clone of the properties specified to the ctor.
     */
    private final Properties properties;
    
    /**
     * An object wrapping the properties specified to the ctor.
     */
    public Properties getProperties() {
        
        return new Properties( properties );
        
    }

    /**
     * The file containing the index segment.
     */
    protected final File file;

    /**
     * Used to correct decode region-based addresses. The
     * {@link IndexSegmentBuilder} encodes region-based addresses using
     * {@link IndexSegmentRegion}. Those addresses are then transparently
     * decoded by this class. The {@link IndexSegment} itself knows nothing
     * about this entire slight of hand.
     */
    private IndexSegmentAddressManager addressManager;
    
    /**
     * See {@link Options#MAX_BYTES_TO_FULLY_BUFFER_NODES}.
     */
    private final long maxBytesToFullyBufferNodes;
    
    /**
     * A buffer containing the disk image of the nodes in the
     * {@link IndexSegment}. While some nodes will be held in memory by the
     * hard reference queue the use of this buffer means that reading a node
     * that has fallen off of the queue does not require any IOs.
     */
    private ByteBuffer buf_nodes;
    
    /**
     * The random access file used to read the index segment.
     */
    private RandomAccessFile raf;

    /**
     * A read-only view of the checkpoint record for the index segment.
     */
    private IndexSegmentCheckpoint checkpoint;

    /**
     * The metadata record for the index segment.
     */
    private IndexMetadata metadata;
    
    protected void assertOpen() {

        if (!open) {
            
            throw new IllegalStateException();
            
        }

    }
    
    /**
     * A read-only view of the checkpoint record for the index segment.
     */
    public final IndexSegmentCheckpoint getCheckpoint() {
        
        assertOpen();

        return checkpoint;
        
    }

    /**
     * The metadata record for the index segment.
     */
    public final IndexMetadata getIndexMetadata() {
    
        assertOpen();
        
        return metadata;
        
    }
    
    /**
     * True iff the store is open.
     */
    private boolean open = false;

    /**
     * Options understood by the {@link IndexSegmentStore}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options {
        
        /**
         * <code>segmentFile</code> - The name of the file to be opened.
         */
        String SEGMENT_FILE = "segmentFile";
        
        /**
         * <code> maxBytesToFullyBufferedNodes</code> - the maximum #of bytes
         * in the node region for which the {@link IndexSegmentStore} will fully
         * buffer (default <code>10485760</code> bytes (10M)). This may be set
         * to a small value, e.g., <code>1L</code>, to disable buffering.
         * When ZERO (<code>0L</code>) the nodes will be fully buffered
         * regardless of the memory requirements. Nodes are buffered when the
         * {@link IndexSegmentStore} is (re-)opened.
         * <p>
         * The nodes in the {@link IndexSegment} are serialized in a contiguous
         * region by the {@link IndexSegmentBuilder}. That region may be fully
         * buffered, in which case queries against the {@link IndexSegment} will
         * incur NO disk hits for the nodes and only one disk hit per visited
         * leaf.
         * 
         * @see #DEFAULT_MAX_BYTES_TO_FULLY_BUFFER_NODES
         */
        String MAX_BYTES_TO_FULLY_BUFFER_NODES = "maxBytesToFullyBufferNodes";
        
        /**
         * @see #MAX_BYTES_TO_FULLY_BUFFER_NODES
         */
        String DEFAULT_MAX_BYTES_TO_FULLY_BUFFER_NODES = ""+Bytes.megabyte*10;
        
    }
    
    /**
     * Used solely to align the old ctor with the new.
     */
    private static Properties getDefaultProperties(File file) {

        if(file==null)
            throw new IllegalArgumentException();

        Properties p = new Properties();
        
        p.setProperty(Options.SEGMENT_FILE, ""+file);
        
        return p;
        
    }
    
    /**
     * Open a read-only store containing an {@link IndexSegment}.
     * 
     * @param file
     *            The name of the store file.
     * 
     * @deprecated by {@link IndexSegmentStore#IndexSegmentStore(Properties)}
     */
    public IndexSegmentStore(File file) {

        this( getDefaultProperties(file));

    }
    
    /**
     * Open a read-only store containing an {@link IndexSegment}.
     * 
     * @param properties
     *            The properties. See {@link Options}.
     * 
     * @see Options
     * @see #loadIndexSegment()
     */
    public IndexSegmentStore(Properties properties) {

        if (properties == null)
            throw new IllegalArgumentException();

        // clone to avoid side effects from modifications by the caller.
        this.properties = (Properties)properties.clone();
        
        // segmentFile
        {

            String val = properties.getProperty(Options.SEGMENT_FILE);

            if (val == null) {

                throw new RuntimeException("Required property not found: "
                        + Options.SEGMENT_FILE);

            }

            file = new File(val);
            
        }
        
        // maxBytesToFullyBufferNodes
        
        {
            
            maxBytesToFullyBufferNodes = Long.parseLong(properties.getProperty(
                    Options.MAX_BYTES_TO_FULLY_BUFFER_NODES,
                    Options.DEFAULT_MAX_BYTES_TO_FULLY_BUFFER_NODES));

            log.info(Options.MAX_BYTES_TO_FULLY_BUFFER_NODES + "="
                    + maxBytesToFullyBufferNodes);
            
            if (maxBytesToFullyBufferNodes < 0L) {

                throw new RuntimeException(
                        Options.MAX_BYTES_TO_FULLY_BUFFER_NODES
                                + " must be non-negative");
                
            }
            
        }
        
        reopen();

    }
    
    /**
     * Closes out the {@link IndexSegmentStore} iff it is still open.
     * <p>
     * Note: The {@link IndexSegment} has hard reference to the
     * {@link IndexSegmentStore} but not the other way around. Therefore an
     * {@link IndexSegment} will be swept before its store is finalized.
     */
    protected void finalize() throws Exception {
        
        if(open) {

            final String name = MetadataService.getIndexPartitionName(metadata
                    .getName(), metadata.getPartitionMetadata()
                    .getPartitionId());

            log.warn("Closing index segment store: " + name + ", file="
                    + getFile());
            
            close();
            
        }
        
    }

    public String toString() {
        
        // @todo add filename if filename dropped from resourcemetadata.
        return getResourceMetadata().toString();
        
    }

    public IResourceMetadata getResourceMetadata() {
        
        if(!open) reopen();

        return new SegmentMetadata(file.toString(), checkpoint.length,
                checkpoint.segmentUUID, checkpoint.commitTime);
        
    }
    
    /**
     * Re-open a closed store. This operation should succeed if the backing file
     * is still accessible.
     * 
     * @exception IllegalStateException
     *                if the store is not closed.
     * 
     * @see #close()
     */
    synchronized public void reopen() {

        if (open)
            throw new IllegalStateException("Already open.");
        
        if (!file.exists()) {

            throw new RuntimeException("File does not exist: "
                    + file.getAbsoluteFile());

        }

        try {

            // open the file.
            this.raf = new RandomAccessFile(file, "r");

            // read the checkpoint record from the file.
            this.checkpoint = new IndexSegmentCheckpoint(raf);

            log.info(checkpoint.toString());

            // handles transparent decoding of offsets within regions.
            this.addressManager = new IndexSegmentAddressManager(checkpoint);
            
            // Read the metadata record.
            this.metadata = readMetadata();

            /*
             * Read the index nodes from the file into a buffer. If there are no
             * index nodes then we skip this step. Note that we always read in
             * the root, so if the index is just a root leaf then the root will
             * be a deserialized object and the file will not be buffered in
             * memory.
             */
            if(checkpoint.nnodes == 0) {
                
                this.buf_nodes = null;
                
            } else {
                
                final long nodesByteCount = getByteCount(checkpoint.addrLeaves);

                final boolean bufferNodes = maxBytesToFullyBufferNodes == 0L
                        || nodesByteCount < maxBytesToFullyBufferNodes;
                
                this.buf_nodes = (bufferNodes ? bufferIndexNodes(raf) : null);

            }

            /*
             * Mark as open so that we can use read(long addr) to read other
             * data (the root node/leaf).
             */
            this.open = true;

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }
    
    /**
     * Load the {@link IndexSegment}. The {@link IndexSegment} (or derived
     * class) MUST provide a public constructor with the following signature:
     * <code>
     * 
     * <i>className</i>(IndexSegmentFileStore store)
     * 
     * </code>
     * 
     * @param store
     *            The store.
     * 
     * @return The {@link IndexSegment} or derived class loaded from that store.
     */
    synchronized public IndexSegment loadIndexSegment() {
        
        try {
            
            Class cl = Class.forName(metadata.getClassName());
            
            Constructor ctor = cl
                    .getConstructor(new Class[] { IndexSegmentStore.class });

            IndexSegment seg = (IndexSegment) ctor
                    .newInstance(new Object[] { this });
            
            return seg;
            
        } catch(Exception ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }
    
    public boolean isOpen() {
        
        return open;
        
    }
   
    public boolean isReadOnly() {

        assertOpen();

        return true;
        
    }
    
    public boolean isStable() {
        
        return true;
        
    }

    /**
     * Return <code>false</code> since the leaves are not fully buffered even
     * if the nodes are fully buffered.
     */
    public boolean isFullyBuffered() {
        
        return false;
        
    }
    
    /**
     * Return <code>true</code> if the nodes of the {@link IndexSegment} are
     * fully buffered in memory.
     */
    public boolean isNodesFullyBuffered() {
        
        return isOpen() && buf_nodes != null;
        
    }
    
    public File getFile() {
        
        return file;
        
    }
    
    /**
     * Closes the file and releases the internal buffers and metadata records.
     * This operation may be reversed by {@link #reopen()} as long as the
     * backing file remains available.
     */
    synchronized public void close() {

        assertOpen();
        
        try {

            raf.close();
            
            raf = null;
            
            buf_nodes = null;

            checkpoint = null;
            
            metadata = null;
            
            open = false;

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }
    
    synchronized public void deleteResources() {
        
        if (open)
            throw new IllegalStateException();
        
        if(!file.delete()) {
            
            throw new RuntimeException("Could not delete: "
                    + file.getAbsolutePath());
            
        }

    }
    
    synchronized public void closeAndDelete() {
        
        close();

        deleteResources();
        
    }

    public long write(ByteBuffer data) {

        throw new UnsupportedOperationException();

    }

    public void force(boolean metadata) {
        
        throw new UnsupportedOperationException();
        
    }
    
    public long size() {

        assertOpen();
        
        return checkpoint.length;
        
    }

    /**
     * @todo report some interesting stats, but most are on the
     *       {@link IndexSegment} itself.
     */
    synchronized public CounterSet getCounters() {
        if(root==null) {
            root = new CounterSet();
        }
        return root;
    }
    private CounterSet root;

    /**
     * Read a record from the {@link IndexSegmentStore}. If the request is in
     * the node region and the nodes have been buffered then this uses a slice
     * on the node buffer. Otherwise this reads through to the backing file.
     * <p>
     * Note: An LRU disk cache is a poor choice for the leaves. Since the btree
     * already maintains a cache of the recently touched leaf objects, a recent
     * read against the disk is the best indication that we have that we will
     * NOT want to read that region again soon.
     */
    public ByteBuffer read(long addr) {

        assertOpen();
        
        final long offset = addressManager.getOffset(addr);

        final int length = addressManager.getByteCount(addr);
        
        final long offsetNodes = addressManager.getOffset(checkpoint.addrNodes);

        ByteBuffer dst;

        if (offset >= offsetNodes && buf_nodes != null) {

            /*
             * the data are buffered. create a slice onto the read-only
             * buffer that reveals only those bytes that contain the desired
             * node. the position() of the slice will be zero(0) and the
             * limit() will be the #of bytes in the compressed record.
             */

            // correct the offset so that it is relative to the buffer.
            long off = offset - offsetNodes;

            // System.err.println("offset="+offset+", length="+length);

            // set the limit on the buffer to the end of the record.
            buf_nodes.limit((int)(off + length));

            // set the position on the buffer to the start of the record.
            buf_nodes.position((int)off);

            // create a slice of that view.
            dst = buf_nodes.slice();

        } else {

            // Allocate buffer.
            dst = ByteBuffer.allocate(length);

            /*
             * the data need to be read from the file.
             */

            dst.limit(length);

            dst.position(0);

            try {

                // read into [dst] - does not modify the channel's position().
                raf.getChannel().read(dst, offset);

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

            dst.flip(); // Flip buffer for reading.

        }

        return dst;

    }

    /**
     * Reads the index nodes into a buffer.
     * 
     * @return A read-only view of a buffer containing the index nodes.
     */
    protected ByteBuffer bufferIndexNodes(RandomAccessFile raf)
            throws IOException {

        if(checkpoint.addrNodes == 0L) {
            
            throw new IllegalStateException("No nodes.");
            
        }
        
        final long offset = addressManager.getOffset(checkpoint.addrNodes);

        final int nbytes = addressManager.getByteCount(checkpoint.addrLeaves);

        log.info("Buffering nodes: #nodes=" + checkpoint.nnodes + ", #bytes="
                + nbytes + ", file=" + file);
        
        /*
         * Note: The direct buffer imposes a higher burden on the JVM and all
         * operations after we read the data from the disk should be faster with
         * a heap buffer, so my expectation is that a heap buffer is the correct
         * choice here.
         */
//        ByteBuffer buf = ByteBuffer.allocateDirect(nbytes);
        ByteBuffer buf = ByteBuffer.allocate(nbytes);

        raf.getChannel().read(buf, offset);

        return buf.asReadOnlyBuffer();

    }

    /**
     * Reads the bloom filter directly from the file.
     * 
     * @return The bloom filter -or- <code>null</code> if the bloom filter was
     *         not constructed when the {@link IndexSegment} was built.
     */
    protected BloomFilter readBloomFilter() throws IOException {

        final long addr = checkpoint.addrBloom;
        
        if(addr == 0L) {
            
            return null;
            
        }
        
        log.info("reading bloom filter: "+addressManager.toString(addr));
        
        final long off = addressManager.getOffset(addr);
        
        final int len = addressManager.getByteCount(addr);
        
        ByteBuffer buf = ByteBuffer.allocate(len);

        buf.limit(len);

        buf.position(0);

        try {

            // read into [dst] - does not modify the channel's position().
            final int nread = raf.getChannel().read(buf, off);
            
            assert nread == len;
            
            buf.flip(); // Flip buffer for reading.
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        assert buf.position() == 0;
        assert buf.limit() == len;

        BloomFilter bloomFilter = (BloomFilter) SerializerUtil.deserialize(buf);

        log.info("Read bloom filter: minKeys=" + bloomFilter.size()
                + ", entryCount=" + checkpoint.nentries + ", bytesOnDisk="
                + len + ", errorRate=" + metadata.getErrorRate());

        return bloomFilter;

    }

    /**
     * Reads the {@link IndexMetadata} record directly from the file.
     */
    protected IndexMetadata readMetadata() throws IOException {

        final long addr = checkpoint.addrMetadata;
        
        assert addr != 0L;
        
        log.info("reading extension metadata record: "+addressManager.toString(addr));
        
        final long off = addressManager.getOffset(addr);
        
        final int len = addressManager.getByteCount(addr);
        
        ByteBuffer buf = ByteBuffer.allocate(len);

        buf.limit(len);

        buf.position(0);

        try {

            // read into [dst] - does not modify the channel's position().
            final int nread = raf.getChannel().read(buf, off);
            
            assert nread == len;
            
            buf.flip(); // Flip buffer for reading.
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        assert buf.position() == 0;
        assert buf.limit() == len;

        final IndexMetadata extensionMetadata = (IndexMetadata) SerializerUtil
                .deserialize(buf);

        log.info("Read extension metadata: " + extensionMetadata);

        return extensionMetadata;

    }

    /*
     * IAddressManager
     */
    
    final public int getByteCount(long addr) {
        return addressManager.getByteCount(addr);
    }

    final public long getOffset(long addr) {
        return addressManager.getOffset(addr);
    }

    final public void packAddr(DataOutput out, long addr) throws IOException {
        addressManager.packAddr(out, addr);
    }

    final public long toAddr(int nbytes, long offset) {
        return addressManager.toAddr(nbytes, offset);
    }

    final public String toString(long addr) {
        return addressManager.toString(addr);
    }

    final public long unpackAddr(DataInput in) throws IOException {
        return addressManager.unpackAddr(in);
    }
    
}
