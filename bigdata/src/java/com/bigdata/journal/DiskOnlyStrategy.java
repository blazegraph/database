package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Disk-based journal strategy.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BufferMode#Disk
 */
public class DiskOnlyStrategy extends AbstractBufferStrategy implements
        IDiskBasedStrategy {
    
    /**
     * The file.
     */
    final File file;

    /**
     * The IO interface for the file.
     */
    final RandomAccessFile raf;

    /**
     * The size of the journal header, including MAGIC, version, and both root
     * blocks. This is used as an offset when computing the address of a record
     * in an underlying file and is ignored by buffer modes that are not backed
     * by a file (e.g., transient) or that are memory mapped (since the map is
     * setup to skip over the header)
     */
    final int headerSize;
    
    /**
     * Extent of the file. This value should be valid since we obtain an
     * exclusive lock on the file when we open it.
     */
    long extent;

    long userExtent;
    
    /**
     * The backing channel.
     */
    final FileChannel channel;

    private boolean open;

    /**
     * Enable or disabled use of Asynchronous IO.
     * 
     * FIXME aio support is not working yet.
     */
    private static final boolean aio = false;
    
    /**
     * A single-threaded service for writing data onto the disk that is
     * used iff {@link #aio} is enabled.
     */
    private ExecutorService writerExecutor;

    /**
     * A write cache used iff {@link #aio} is enabled. The keys are the long
     * offsets into the file. The values are the buffered writes.
     */
    private Map<Long,ByteBuffer> writeCache;
    
    final public int getHeaderSize() {
        
        return headerSize;
        
    }
    
    final public File getFile() {
        
        return file;
        
    }
    
    final public RandomAccessFile getRandomAccessFile() {

        return raf;
        
    }

    DiskOnlyStrategy(long maximumExtent, FileMetadata fileMetadata) {

        super(fileMetadata.extent, maximumExtent, fileMetadata.nextOffset,
                BufferMode.Disk);

        this.file = fileMetadata.file;

        this.raf = fileMetadata.raf;

        this.extent = fileMetadata.extent;

        this.headerSize = fileMetadata.headerSize0;
        
        this.userExtent = extent - headerSize;
        
        this.channel = raf.getChannel();

        startWriter();

        open = true;

    }

    /**
     * Starts a single-threaded writer iff {@link #aio} is in use.
     */
    private final void startWriter() {

        if(!aio) return;

        if(writeCache==null) {
            
            writeCache = new ConcurrentHashMap<Long, ByteBuffer>();
            
        }
        
        /*
         * @todo javadoc based on the text below.
         * 
         * @todo modify to support aio (asynchronous io). aio can be supported
         * with a cache of recently written buffers and 2nd thread that writes
         * behind from the cache to the file. for the disk-only mode, we would
         * have to add a hash-based lookup (on the offset) of the recently
         * written buffers so that reads test the cache before reading from the
         * disk. force() will need to be modified to wait until the aio thread
         * has caught up to the nextOffset (i.e., has written all data that is
         * dirty on the buffer).
         * 
         * @todo modify to do better than synchronized on read/write in support
         * or MROW, e.g., by using AIO in conjunction with a set of write
         * buffers and a hash map into those based on addresses. E.g., a ring
         * buffer over an array of ByteBuffers. Data are copied into the next
         * slot in the ring buffer on write and flushed through to the disk
         * periodically or by force(). The buffers are also inserted into a hash
         * map for lookup by address for higher read concurrency. (We could also
         * buffer recent reads using a similar mechanism which would help to
         * remove any hotspots in the disk, but the disk cache might already
         * cover those so test that both ways).
         */

        // Start a single-threaded writer.
        writerExecutor = Executors.newSingleThreadExecutor(DaemonThreadFactory
                .defaultThreadFactory());

    }
    
    /**
     * Forces the writer to shutdown normally which has the effect of causing
     * all pending writes to execute while not allowing new writes.  This is
     * used by {@link #force(boolean)} to force pending writes through to disk.
     */
    private final void closeWriter() {
        
        if(!aio) return;

        // force the writer to terminate.
        writerExecutor.shutdown();
        
        assert writeCache.isEmpty();
        
    }

    /**
     * Task writes a record onto the disk.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo modify to aggregate writes and use a single NIO operation for
     *       several writes.
     */
    private class WriterTask implements Callable<Void> {

        private final long pos;
        private final ByteBuffer data;
        
        /**
         * 
         * @param pos
         *            The offset into the file at which the data will be
         *            written.
         * @param data
         *            The data are cloned to prevent concurrent modification by
         *            the application (this is per the general contract for
         *            {@link IRawStore#write(ByteBuffer)}.
         */
        public WriterTask(long pos, ByteBuffer data) {
            
            this.pos = pos;
            
            this.data = ByteBuffer.allocate(data.remaining());
            
            this.data.put(data);
            
            writeCache.put(pos, this.data);
            
        }
        
        public Void call() throws Exception {

            final int nbytes = data.remaining();

            if(data != writeCache.remove(pos)) {

                throw new AssertionError();

            }

            /*
             * Write the buffer on the disk.
             * 
             * Note: This duplicates the buffer for the operation so that
             * concurrent read operations can not effect its state (position and
             * limit).
             */ 
            final int count = raf.getChannel().write(data.asReadOnlyBuffer(),
                    pos);
        
            if(count != nbytes) {
                
                throw new IOException("Expecting to write " + nbytes
                        + " bytes, but wrote " + count + " bytes.");
                
            }
            
            return null;
            
        }
        
    }
    
    final public boolean isOpen() {

        return open;

    }

    final public boolean isStable() {
        
        return true;
        
    }

    public boolean isFullyBuffered() {
        
        return false;
        
    }
    
    /**
     * Forces the data to disk.
     */
    public void force(boolean metadata) {

        // flush all pending writes to disk.
        closeWriter();
        
        try {

            try {

                raf.getChannel().force(metadata);

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

        } finally {

            // restart the writer.
            startWriter();

        }
        
    }

    /**
     * Closes the file immediately (without flushing any pending writes).
     */
    public void close() {

        if (! open )
            throw new IllegalStateException();
        
        try {

            raf.close();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        open = false;

    }

    public void deleteFile() {
        
        if( open ) throw new IllegalStateException();
        
        if( ! file.delete() ) {
            
            throw new RuntimeException("Could not delete file: "
                    + file.getAbsoluteFile());
            
        }
        
    }
    
    final public long getExtent() {

        return extent;

    }

    final public long getUserExtent() {
        
        return userExtent;
        
    }

    public ByteBuffer read(long addr) {

        if (addr == 0L)
            throw new IllegalArgumentException("Address is 0L");

        final int offset = Addr.getOffset(addr);

        final int nbytes = Addr.getByteCount(addr);

        if (nbytes == 0) {

            throw new IllegalArgumentException(
                    "Address encodes record length of zero");

        }

        try {

            if (offset + nbytes > nextOffset) {

                throw new IllegalArgumentException("Address never written.");

            }

            final long pos = (long) offset + headerSize;

            final ByteBuffer dst;

            // copy the data into the buffer.

            if (aio) {
             
                // check the write cache.
                ByteBuffer buf = writeCache.get(pos);

                if (buf != null) {

                    /*
                     * Duplicate the buffer so that callers can not side effect
                     * the buffer state (position and limit) while it is waiting
                     * to be written to the disk.
                     */

                    return buf.asReadOnlyBuffer();

                } else {

                    /*
                     * Read from the disk.
                     */

                    // allocate a new buffer of the exact capacity.
                    dst = ByteBuffer.allocate(nbytes);

                    raf.getChannel().read(dst, pos);

                }

            } else {

                // allocate a new buffer of the exact capacity.
                dst = ByteBuffer.allocate(nbytes);

                synchronized (this) {

                    raf.getChannel().read(dst, pos);

                }

            }

            // flip for reading.

            dst.flip();

            // return the buffer.

            return dst;

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
        
    }

    public long write(ByteBuffer data) {

        if (data == null)
            throw new IllegalArgumentException("Buffer is null");

        // #of bytes to store.
        final int nbytes = data.remaining();

        if (nbytes == 0)
            throw new IllegalArgumentException("No bytes remaining in buffer");

        try {

            long pos = (long) nextOffset + (long) headerSize;

            if (pos + nbytes > Integer.MAX_VALUE) {

                throw new IOException("Would exceed int32 bytes in file.");

            }

            final long needed = (nextOffset + nbytes) - userExtent;

            if (needed > 0) {

                if (!overflow((int) needed)) {

                    throw new OverflowException();

                }

            }

            /*
             * The offset at which the record will be written (not adjusted for
             * the root blocks).
             */
            final int offset = nextOffset;

            if (aio) {

                /*
                 * Queue up a write.
                 */
                
                writerExecutor.submit(new WriterTask(pos, data));
                
            } else synchronized(this) {

                /* 
                 * Write the data synchronously onto the end of the file.
                 */

                final int count = raf.getChannel().write(data, pos);

                if (count != nbytes) {

                    throw new IOException("Expecting to write " + nbytes
                            + " bytes, but wrote " + count + " bytes.");

                }
                
            }

            // increment by the #of bytes written.
            nextOffset += nbytes;
            
            // formulate the address that can be used to recover that record.
            return Addr.toLong(nbytes, offset);

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }
    
    public void writeRootBlock(IRootBlockView rootBlock,ForceEnum forceOnCommit) {

        if (rootBlock == null)
            throw new IllegalArgumentException();
        
        try {

            FileChannel channel = raf.getChannel();

            final int count = channel.write(rootBlock.asReadOnlyBuffer(),
                    rootBlock.isRootBlock0() ? FileMetadata.OFFSET_ROOT_BLOCK0
                            : FileMetadata.OFFSET_ROOT_BLOCK1);
            
            if(count != RootBlockView.SIZEOF_ROOT_BLOCK) {
                
                throw new IOException("Expecting to write "
                        + RootBlockView.SIZEOF_ROOT_BLOCK + " bytes, but wrote"
                        + count + " bytes.");
                
            }

            if( forceOnCommit != ForceEnum.No ) {

                force(forceOnCommit==ForceEnum.ForceMetadata);
            
            }

        }

        catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    public void truncate(long newExtent) {

        long newUserExtent =  newExtent - headerSize;
        
        if (newUserExtent < getNextOffset() ) {
           
            throw new IllegalArgumentException("Would truncate written data.");
            
        }

        if (newUserExtent > Integer.MAX_VALUE) {

            throw new IllegalArgumentException("User extent would exceed int32 bytes");
            
        }
        
        if(newUserExtent == getUserExtent()) {
            
            // NOP.
            return;
            
        }
        
        try {

            // extend the file.
            raf.setLength(newExtent);
            
            /*
             * since we just changed the file length we force the data to disk
             * and update the file metadata. this is a relatively expensive
             * operation but we want to make sure that we do not loose track of
             * a change in the length of the file.
             * 
             * @todo an alternative would be to set a marker on the buffer such
             * that the next force() also forced the metadata to disk.
             */
            force(true);

            System.err.println("Disk file: newLength="+cf.format(newExtent));
                        
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
 
        this.userExtent = newUserExtent;
        
        this.extent = newExtent;
        
    }

    public long transferTo(RandomAccessFile out) throws IOException {
        
        return super.transferFromDiskTo(this, out);
        
    }
    
}
