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
package com.bigdata.journal;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.bigdata.counters.CounterSet;
import com.bigdata.io.FileChannelUtility;

/**
 * Implements logic to read from and write on a buffer. This is sufficient
 * for a {@link BufferMode#Transient} implementation or a
 * {@link BufferMode#Mapped} implementation, but the
 * {@link BufferMode#Direct} implementation needs to also implement write
 * through to the disk.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class BasicBufferStrategy extends AbstractBufferStrategy {

    /**
     * A buffer containing a write through image of the backing file. The image
     * begins after the root blocks, making it impossible to write on the root
     * blocks using the buffer. The offset of the image into the backing file is
     * given by {@link FileMetadata#headerSize0}.
     * <p>
     * Note: The {@link #buffer} reference is updated by
     * {@link #truncate(long)}. Since both readers and writers MUST use the
     * current value for this variable it is marked as <code>volatile</code>.
     */
    private volatile ByteBuffer buffer;
    
    protected ByteBuffer getBuffer() {
        
        return buffer;
        
    }
    
    /**
     * 
     * @param readOnly
     * @return
     */
    protected ByteBuffer getBufferView(boolean readOnly) {

        synchronized (buffer) {

            if (readOnly) {

                return buffer.asReadOnlyBuffer();

            } else {

                return buffer.duplicate();

            }

        }

    }

    /**
     * The size of the journal header, including MAGIC, version, and both root
     * blocks. This is used as an offset when computing the address of a record
     * in an underlying file and is ignored by buffer modes that are not backed
     * by a file (e.g., transient) or that are memory mapped (since the map is
     * setup to skip over the header)
     */
    final int headerSize;
    
    /**
     * The current length of the backing file in bytes.
     */
    protected long extent;

    /**
     * The size of the user data space in bytes.
     */
    protected long userExtent;
    
    public long getExtent() {

        return extent;

    }

    public long getUserExtent() {
        
        return userExtent;
        
    }

    BasicBufferStrategy(long maximumExtent, int offsetBits, long nextOffset,
            int headerSize, long extent, BufferMode bufferMode,
            ByteBuffer buffer, boolean readOnly) {

        super(extent, maximumExtent, offsetBits, nextOffset, bufferMode, readOnly);

        this.buffer = buffer;

        this.extent = extent;
        
        this.headerSize = headerSize;
        
        this.userExtent = extent - headerSize;

    }

    /**
     * Releases the buffer.
     */
    public void close() {

        super.close();
        
        // release the buffer.
        buffer = null;

    }
    
    public long write(ByteBuffer data) {
        
        if (data == null)
            throw new IllegalArgumentException(ERR_BUFFER_NULL);

        if (isReadOnly())
            throw new IllegalStateException(ERR_READ_ONLY);
        
        // #of bytes to store.
        final int nbytes = data.remaining();

        if (nbytes == 0)
            throw new IllegalArgumentException(ERR_BUFFER_EMPTY);

        /*
         * Write the record on the buffer.
         * 
         * Note: This is synchronized in order to support concurrent writers.
         */
        
        final long addr;
        synchronized(this) 
        {
            
            // the next offset.
            final long offset = nextOffset;
            
            // formulate the address that can be used to recover that record.
            addr = toAddr(nbytes, offset);

            final long needed = (offset + nbytes) - userExtent;

            if (offset + nbytes > Integer.MAX_VALUE) {

                throw new RuntimeException(ERR_INT32);

            }

            if (needed > 0) {

                if (!overflow(needed)) {

                    throw new OverflowException();

                }

            }

            /*
             * Note: The data MUST be copied within the synchronized() block
             * since otherwise overflow() could cause the buffer reference to be
             * invalidated. (Likewise, we do not "own" the limit and position on
             * the [buffer] unless everyone is synchronized.)
             */
            
            buffer.limit((int) offset + nbytes);
            buffer.position((int) offset);

            buffer.put(data);

            // increment by the #of bytes written.
            nextOffset += nbytes;

        }

        return addr;

    }

    public ByteBuffer read(long addr) {
        
        if (addr == 0L)
            throw new IllegalArgumentException(ERR_ADDRESS_IS_NULL);
        
        final long offset = getOffset(addr);
        
        final int nbytes = getByteCount(addr);

        if(nbytes==0) {
            
            throw new IllegalArgumentException(ERR_RECORD_LENGTH_ZERO);
            
        }
        
        if (offset + nbytes > nextOffset) {
            
            throw new IllegalArgumentException(ERR_ADDRESS_NOT_WRITTEN);

        }
        
        /*
         * Create a view onto the same buffer with distinct position, limit and
         * mark. This allows us to perform concurrent reads on the buffer which
         * in turn supports concurrent execution of concurrent transactions
         * reading from various historical states.
         */
        final ByteBuffer view;
        synchronized(this) {
            /*
             * Note: Synchronization is required to have asReadOnlyBuffer()
             * construct a consistent view with concurrent writers. If you do
             * not synchronize then an IllegalArgumentException can get tossed
             * out of here. The problem is that the operation is not atomic
             * without synchronization (concurrent operations can modify the
             * limit and position rendering the result inconsistent). This
             * problem was revealed by the AbstractMRMWTestCase, but it does not
             * show up on every run.
             */
            view = buffer.asReadOnlyBuffer();
        }

        // return a read-only view onto the data in the store.

        view.limit((int) offset + nbytes);
        view.position((int) offset);

        // the slice will show only the record being read.
        
        return view.slice();

    }

    /**
     * Note: This is synchronized since it MAY be invoked directly while
     * concurrent writers are running and not just from {@link #overflow(long)}.
     */
    synchronized public void truncate(final long newExtent) {

        final long newUserExtent = newExtent - headerSize;
        
        if (newUserExtent < getNextOffset() ) {
           
            throw new IllegalArgumentException(ERR_TRUNCATE);
            
        }

        if (newUserExtent > Integer.MAX_VALUE) {

            /*
             * Constraint when using a buffered mode.
             */
            
            throw new IllegalArgumentException(ERR_INT32);
            
        }
        
        if(newUserExtent == getUserExtent()) {
            
            // NOP.
            return;
            
        }
        
        final boolean isDirect = buffer.isDirect();

        final int newCapacity = (int) newUserExtent;
        
        ByteBuffer tmp = (isDirect?ByteBuffer.allocateDirect(newCapacity):
            ByteBuffer.allocate(newCapacity)
            );
        
        /*
         * Copy at most those bytes that have been written on.
         */
        buffer.limit((int)Math.min(nextOffset,newCapacity));
        buffer.position(0);
        
        // Copy to the new buffer.
        tmp.put(buffer);

        // Replace the buffer reference.
        buffer = tmp;
        
        extent = newUserExtent + headerSize;
        
        userExtent = newUserExtent;
        
        if(INFO)
            log.info("Buffer: newCapacity=" + cf.format(newCapacity));

    }
    
    /**
     * Note: This is synchronized so that concurrent writers must block during
     * this operation.
     */
    synchronized public long transferTo(RandomAccessFile out)
            throws IOException {
        
        final long count = nextOffset;
        
        final FileChannel outChannel = out.getChannel();
        
        // current position on the output channel.
        final long toPosition = outChannel.position();

//        if(toPosition + count > Integer.MAX_VALUE) {
//            
//            throw new IOException("Would exceed int32 bytes.");
//            
//        }
        
        /*
         * Write all the data onto the output channel.
         */
        
//        final long begin = System.currentTimeMillis();
        
        // setup the buffer for the operation.
        buffer.limit((int)nextOffset);
        buffer.position(0);
        
        FileChannelUtility.writeAll(outChannel, buffer, toPosition);

        outChannel.position(toPosition+count);

//        // write the data : @todo use an explicit position for this write?
//        final long nwritten = outChannel.write(buffer);
//        
//        if( nwritten != count ) {
//            
//            throw new AssertionError("Expected to write " + count
//                    + " bytes but wrote " + nwritten);
//            
//        }
//
//        final long elapsed = System.currentTimeMillis() - begin;
//        
//        log.info("\nTransferred " + count
//                + " bytes from memory to disk at offset=" + toPosition + " in "
//                + elapsed + "ms");

        return count;
        
    }
    
    /**
     * FIXME Counters need to be added here for the {@link DirectBufferStrategy},
     * {@link MappedBufferStrategy}, and {@link TransientBufferStrategy}.
     */
    synchronized public CounterSet getCounters() {

        if (root == null) {

            root = new CounterSet();

        }

        return root;

    }
    private CounterSet root;

}
