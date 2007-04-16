/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
package com.bigdata.journal;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.bigdata.rawstore.Addr;

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
     * given by {@link AbstractBufferStrategy#headerSize}.
     */
    protected ByteBuffer directBuffer;

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

    BasicBufferStrategy(long maximumExtent, int nextOffset, int headerSize,
            long extent, BufferMode bufferMode, ByteBuffer buffer) {

        super(extent, maximumExtent, nextOffset, bufferMode);

        this.directBuffer = buffer;

        this.extent = extent;
        
        this.headerSize = headerSize;
        
        this.userExtent = extent - headerSize;

    }

    public long write(ByteBuffer data) {
        
        if (data == null)
            throw new IllegalArgumentException("Buffer is null");

        // #of bytes to store.
        final int nbytes = data.remaining();

        if (nbytes == 0)
            throw new IllegalArgumentException("No bytes remaining in buffer");

        // the next offset.
        final int offset = nextOffset;
        
        final long needed = (offset + nbytes) - userExtent;

        if (needed > 0) {

            if (!overflow((int) needed)) {
                
                throw new OverflowException();
                
            }
            
        }
       
        directBuffer.limit(offset + nbytes);
        directBuffer.position(offset);
        
        directBuffer.put(data);
        
        // increment by the #of bytes written.
        nextOffset += nbytes;
        
        // formulate the address that can be used to recover that record.
        long addr = Addr.toLong(nbytes, offset);

        return addr;

    }

    public ByteBuffer read(long addr) {
        
        if (addr == 0L)
            throw new IllegalArgumentException("Address is 0L");
        
        final int offset = Addr.getOffset(addr);
        
        final int nbytes = Addr.getByteCount(addr);

        if(nbytes==0) {
            
            throw new IllegalArgumentException(
                    "Address encodes record length of zero");
            
        }
        
        if (offset + nbytes > nextOffset) {
            
            throw new IllegalArgumentException("Address never written.");

        }
        
        /*
         * Create a view onto the same buffer with distinct position, limit and
         * mark. This allows us to perform concurrent reads on the buffer which
         * in turn supports concurrent execution of concurrent transactions
         * reading from various historical states.
         */
        ByteBuffer view = directBuffer.asReadOnlyBuffer();

        // return a read-only view onto the data in the store.

        view.limit(offset + nbytes);
        view.position(offset);

        return view.slice();

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
        
        final boolean isDirect = directBuffer.isDirect();

        final int newCapacity = (int) newUserExtent;
        
        ByteBuffer tmp = (isDirect?ByteBuffer.allocateDirect(newCapacity):
            ByteBuffer.allocate(newCapacity)
            );
        
        /*
         * Copy at most those bytes that have been written on.
         */
        directBuffer.limit(Math.min(nextOffset,newCapacity));
        directBuffer.position(0);
        
        // Copy to the new buffer.
        tmp.put(directBuffer);
     
        // Replace the buffer reference.
        directBuffer = tmp;
        
        extent = newUserExtent + headerSize;
        
        userExtent = newUserExtent;
        
        System.err.println("Buffer: newCapacity=" + cf.format(newCapacity));

    }
    
    public long transferTo(RandomAccessFile out) throws IOException {
        
        long count = nextOffset;
        
        final FileChannel outChannel = out.getChannel();
        
        // current position on the output channel.
        final long toPosition = outChannel.position();

        if(toPosition + count > Integer.MAX_VALUE) {
            
            throw new IOException("Index segment exceeds int32 bytes.");
            
        }
        
        /*
         * use a single nio operation to write all the data onto the output
         * channel.
         */
        
        final long begin = System.currentTimeMillis();
        
        // setup the buffer for the operation.
        directBuffer.limit(nextOffset);
        directBuffer.position(0);
        
        // write the data.
        final long nwritten = outChannel.write(directBuffer);
        
        if( nwritten != count ) {
            
            throw new AssertionError("Expected to write " + count
                    + " bytes but wrote " + nwritten);
            
        }

        final long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println("\nTransferred " + count
                + " bytes from memory to disk at offset=" + toPosition + " in "
                + elapsed + "ms");

        return count;
        
    }

}
