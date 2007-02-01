package com.bigdata.objndx;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * A read-only store backed by a file. The section of the file containing
 * the index nodes may be fully buffered.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexSegmentFileStore implements IRawStore2 {

    /**
     * A buffer containing the disk image of the nodes in the index segment.
     * While some nodes will be held in memory by the hard reference queue
     * the use of this buffer means that reading a node that has fallen off
     * of the queue does not require any IOs.
     */
    protected final ByteBuffer buf_nodes;

    /**
     * The file containing the index segment.
     */
    protected final File file;

    /**
     * The random access file used to read the index segment.
     */
    protected final RandomAccessFile raf;

    /**
     * A read-only view of the metadata record for the index segment.
     */
    protected final IndexSegmentMetadata metadata;

    /**
     * True iff the store is open.
     */
    private boolean open = false;

    /**
     * Open the read-only store.
     * 
     * @param file
     * 
     * @throws IOException
     * 
     * @todo make it optional to fully buffer the index nodes?
     * @todo make it optional to fully buffer the entire file.
     * @todo hide IOException in constructor?
     */
    public IndexSegmentFileStore(File file) throws IOException {

        if (file == null)
            throw new IllegalArgumentException();

        this.file = file;

        if (!file.exists()) {

            throw new IOException("File does not exist: "
                    + file.getAbsoluteFile());

        }

        // open the file.
        this.raf = new RandomAccessFile(file, "r");

        // read the metadata record from the file.
        this.metadata = new IndexSegmentMetadata(raf);

        IndexSegment.log.info(metadata.toString());

        /*
         * Read the index nodes from the file into a buffer. If there are no
         * index nodes then we skip this step. Note that we always read in
         * the root, so if the index is just a root leaf then the root will
         * be a deserialized object and the file will not be buffered in
         * memory.
         */
        this.buf_nodes = (metadata.nnodes > 0 ? bufferIndexNodes(raf) : null);

        this.open = true;

    }

    public boolean isOpen() {
        
        return open;
        
    }
   
    public void close() {

        if (!open)
            throw new IllegalStateException();

        try {

            raf.close();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        open = false;

    }

    public void delete(long addr) {

        throw new UnsupportedOperationException();

    }

    public long write(ByteBuffer data) {

        throw new UnsupportedOperationException();

    }

    public void force(boolean metadata) {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Read from the index segment. If the request is in the node region and
     * the nodes have been buffered then this uses a slice on the node
     * buffer. Otherwise this reads through to the backing file.
     */
    public ByteBuffer read(long addr, ByteBuffer dst) {

        if (!open)
            throw new IllegalStateException();

        final int offset = Addr.getOffset(addr);

        final int length = Addr.getByteCount(addr);
        
        final int offsetNodes = Addr.getOffset(metadata.addrNodes);

        if (offset >= offsetNodes && buf_nodes != null) {

            /*
             * the data are buffered. create a slice onto the read-only
             * buffer that reveals only those bytes that contain the desired
             * node. the position() of the slice will be zero(0) and the
             * limit() will be the #of bytes in the compressed record.
             */

            // correct the offset so that it is relative to the buffer.
            int off = offset - offsetNodes;

            // System.err.println("offset="+offset+", length="+length);

            // set the limit on the buffer to the end of the record.
            buf_nodes.limit(off + length);

            // set the position on the buffer to the start of the record.
            buf_nodes.position(off);

            // create a slice of that view.
            dst = buf_nodes.slice();

        } else {

            /*
             *  Allocate if not provided by the caller.
             */
            if (dst == null) {

                dst = ByteBuffer.allocate(length);

            }

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
     * 
     * @todo compare use of direct vs heap ByteBuffer for performance. The
     *       direct buffer imposes a higher burden on the JVM and all operations
     *       after we read the data from the disk should be faster with a heap
     *       buffer, so my expectation is that a heap buffer is the correct
     *       choice here.
     */
    protected ByteBuffer bufferIndexNodes(RandomAccessFile raf)
            throws IOException {

        if(metadata.addrNodes == 0L) {
            
            throw new IllegalStateException("No nodes.");
            
        }
        
        final int offset = Addr.getOffset(metadata.addrNodes);

        final int nbytes = Addr.getByteCount(metadata.addrLeaves);

//        ByteBuffer buf = ByteBuffer.allocateDirect(nbytes);
        ByteBuffer buf = ByteBuffer.allocate(nbytes);

        raf.getChannel().read(buf, offset);

        return buf.asReadOnlyBuffer();

    }

}
