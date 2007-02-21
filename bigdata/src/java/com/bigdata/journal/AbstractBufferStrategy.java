package com.bigdata.journal;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;

import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.Bytes;


/**
 * Abstract base class for {@link IBufferStrategy} implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractBufferStrategy implements IBufferStrategy {
    
    protected final long initialExtent;
    protected final long maximumExtent;
    
    /**
     * The buffer strategy implemented by this class.
     */
    protected final BufferMode bufferMode;
    
    /**
     * The next offset at which a data item would be written on the store as an
     * offset into the <em>user extent</em>. This is updated each time a new
     * record is written on the store. On restart, the value is initialized from
     * the current root block. The current value is written as part of the new
     * root block during each commit.
     * <p>
     * Note: It is NOT safe to reload the current root block and therefore reset
     * this to an earlier offset unless all transactions are discarded. The
     * reason is that transactions may use objects (btrees) to provide
     * isolation. Those objects write on the store but do not register as
     * {@link ICommitter}s and therefore never make themselves restart safe.
     * However, you can not discard the writes of those objects unless the
     * entire store is being restarted, e.g., after a shutdown or a crash.
     */
    protected int nextOffset;

    static final NumberFormat cf;
    
    static {
        
        cf = NumberFormat.getIntegerInstance();
        
        cf.setGroupingUsed(true);
        
    }
    
    final public long getInitialExtent() {
        
        return initialExtent;
        
    }
    
    final public long getMaximumExtent() {
        
        return maximumExtent;
        
    }
    
    final public BufferMode getBufferMode() {

        return bufferMode;
        
    }

    final public int getNextOffset() {

        return nextOffset;
        
    }
    
    /**
     * (Re-)open a buffer.
     * 
     * @param nextOffset
     *            The next offset within the buffer on which a record will be
     *            written. Note that the buffer begins _after_ the root blocks
     *            and offset zero is always the first byte in the buffer.
     * @param bufferMode
     *            The {@link BufferMode}.
     */
    AbstractBufferStrategy(long initialExtent, long maximumExtent,
            int nextOffset, BufferMode bufferMode) {

        assert nextOffset >= 0;
        
        if( bufferMode == null ) throw new IllegalArgumentException();

        this.initialExtent = initialExtent;
        
        this.maximumExtent = maximumExtent;
        
        this.nextOffset = nextOffset;
        
        this.bufferMode = bufferMode;
        
    }

    /**
     * Invoked if the store would overflow on {@link #write(ByteBuffer)}. The
     * default behavior extends the capacity of the buffer by the maximum of 32M
     * or the {@link Options#INITIAL_EXTENT} up to a maximum capacity of
     * {@link Integer#MAX_VALUE} bytes.
     * 
     * @return true if the capacity of the store was extended and the write
     *         operation should be retried.
     */
    public boolean overflow(int needed) {

        final long userExtent = getUserExtent();
        
        final long required = userExtent + needed;
        
        if ( required > Integer.MAX_VALUE) {
            
            // Would overflow int32 bytes.
            
            return false;
            
        }
        
        if( required > maximumExtent ) {
            
            // Would exceed the maximum extent.
            
            return false;
            
        }
        
        /*
         * Increase by the initial extent or by 32M, whichever is greater.
         */
        long newExtent = userExtent
                + Math.max(initialExtent, Bytes.megabyte * 32);
        
        if( newExtent > Integer.MAX_VALUE) {

            // Do not allocate more than int32 bytes.
            newExtent = Integer.MAX_VALUE;
            
        }

        /*
         * Extend the capacity.
         */
        truncate( newExtent );
        
        // Retry the write operation.
        return true;
        
    }
    
    /**
     * Throws an exception if the extent is too large for an in-memory buffer.
     * 
     * @param extent
     *            The extent.
     * 
     * @return The extent.
     * 
     * @deprecated All buffer modes are now limited to int32 offsets since the
     *             offset is encoded in the high word of an {@link Addr}. It is
     *             possible that a disk-only mode could address a longer extent
     *             simply because the offset + nbytes could run beyond the int32
     *             boundary, but that is not worth it.
     */
    static long assertNonDiskExtent(long extent) {

        if( extent > Integer.MAX_VALUE ) {
            
            /*
             * The file image is too large to address with an int32. This
             * rules out both the use of a direct buffer image and the use
             * of a memory-mapped file. Therefore, the journal must use a
             * disk-based strategy.
             */
           
            throw new RuntimeException(
                    "The extent requires the 'disk' mode: extent=" + extent);
            
        }

        return extent;
        
    }

    /**
     * Helper method used by {@link DiskBackedBufferStrategy} and
     * {@link DiskOnlyStrategy} to implement
     * {@link IBufferStrategy#transferTo(RandomAccessFile)}
     * 
     * @param src
     *            The source.
     * @param out
     *            The output file.
     *            
     * @return The #of bytes transferred.
     * 
     * @throws IOException
     */
    protected long transferFromDiskTo(IDiskBasedStrategy src,RandomAccessFile out) throws IOException {

        final long begin = System.currentTimeMillis();
        
        // #of bytes to transfer.
        final long count = src.getNextOffset();

        // the output channel.
        final FileChannel outChannel = out.getChannel();
        
        // current position on the output channel.
        final long toPosition = outChannel.position();
        
        if(toPosition + count > Integer.MAX_VALUE) {
            
            throw new IOException("Index segment exceeds int32 bytes.");
            
        }

        /* 
         * Transfer data from channel to channel.
         */
        
        final FileChannel tmpChannel = src.getRandomAccessFile().getChannel();
        
        /*
         * Set the fromPosition on source channel. We want everything after the
         * file header.
         */
        tmpChannel.position(src.getHeaderSize());

        /*
         * Extend the output file. This is required at least for some
         * circumstances.
         */
        out.setLength(toPosition+count);
                    
        /*
         * Transfer the data. It is possible that this will take multiple
         * writes for at least some implementations.
         */

//        System.err.println("fromPosition="+tmpChannel.position()+", toPosition="+toPosition+", count="+count);

        int nwrites = 0; // #of write operations.

        {
            
            long n = count;
            
            long to = toPosition;
            
            while (n > 0) {

                long nxfer = outChannel.transferFrom(tmpChannel, to, n);
                
                to += nxfer;
                
                n -= nxfer;
                
                nwrites++;
        
//        // Verify transfer is complete.
//            if (nxfer != count) {
//
//                throw new IOException("Expected to transfer " + count
//                        + ", but transferred " + nxfer);
//
//            }

            }
            
        }
        
        /*
         * Update the position on the output channel since transferFrom does
         * NOT do this itself.
         */
        outChannel.position(toPosition+count);
        
        final long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println("\nTransferred " + count
                + " bytes from disk channel to disk channel (offset="
                + toPosition + ") in " + nwrites + " writes and " + elapsed
                + "ms");

        return count;
        
    }
    
}
