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
/*
 * Created on Feb 15, 2007
 */

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;

/**
 * A non-restart-safe store for temporary data that buffers data in memory until
 * a maximum capacity has been reached and then converts to a disk-based store
 * with a maximum capacity of 2G. The maximum capacity constraint is imposed by
 * {@link Addr}. On conversion to a disk-backed store, the disk file is created
 * using the temporary file mechansism and is marked for eventual deletion no
 * later than when the JVM exits and as soon as the store is {@link #close()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo The {@link TemporaryStore} would benefit from any caching or AIO
 *       solutions developed for the {@link DiskOnlyStrategy}.
 */
public class TemporaryStore implements IRawStore {

    /**
     * The initial size of the in-memory buffer. This buffer will grow as
     * necessary until {@link #maximumInMemoryExtent} at which point the store
     * will convert over to a disk-based mechanism.
     */
    public final long initialInMemoryExtent;
    
    /**
     * The maximum capacity of the in-memory buffer.
     */
    public final long maximumInMemoryExtent;
    
    /**
     * Whether or not a direct buffer was used for the in-memory store (not
     * recommended).
     */
    public final boolean useDirectBuffers;

    private boolean open = true;
    private IBufferStrategy buf;

    public IBufferStrategy getBufferStrategy() {
        
        return buf;
        
    }

    /**
     * Create a {@link TemporaryStore} with an initial in-memory capacity of 10M
     * that will grow up to 100M before converting into a disk-based store
     * backed by a temporary file.
     */
    public TemporaryStore() {

        this( Bytes.megabyte*10, Bytes.megabyte*100, false );
        
    }

    /**
     * Create a {@link TemporaryStore} with the specified configuration.
     * 
     * @param initialInMemoryExtent
     *            The initial size of the in-memory buffer. This buffer will
     *            grow as necessary until <i>maximumInMemoryExtent</i> at which
     *            point the store will convert over to a disk-based mechanism.
     * @param maximumInMemoryExtent
     *            The maximum capacity of the in-memory buffer. The actual
     *            maximum may differ slightly based on the buffer growth policy.
     * @param useDirectBuffers
     *            Whether or not the in-memory buffer will be direct. The use of
     *            a direct buffer here is NOT recommended.
     */
    public TemporaryStore(long initialInMemoryExtent,
            long maximumInMemoryExtent, boolean useDirectBuffers) {
        
        buf = new TransientBufferStrategy(initialInMemoryExtent, maximumInMemoryExtent,
                useDirectBuffers);
        
        this.initialInMemoryExtent = initialInMemoryExtent;
        
        this.maximumInMemoryExtent = maximumInMemoryExtent;
        
        this.useDirectBuffers = useDirectBuffers;
        
    }

    /**
     * Close the store and delete the associated file, if any.
     */
    public void close() {

        if(!open) throw new IllegalStateException();
        
        open = false;
        
        buf.close();
        
        buf.deleteFile();
        
    }

    public void force(boolean metadata) {
        
        if(!open) throw new IllegalStateException();
        
        buf.force(metadata);
        
    }

    public boolean isOpen() {
        
        return open;
        
    }

    /**
     * Always returns <code>false</code> since the store will be deleted as soon
     * as it is closed.
     */
    public boolean isStable() {
        return false;
    }

    public ByteBuffer read(long addr, ByteBuffer dst) {

        if(!open) throw new IllegalStateException();

        return buf.read(addr, dst);
        
    }

    public long write(ByteBuffer data) {
        
        if(!open) throw new IllegalStateException();
        
        try {
            
            return buf.write(data);
            
        } catch(OverflowException ex) {
        
            if(buf instanceof TransientBufferStrategy) {

                overflowToDisk();
                
                return buf.write(data);
                
            } else {
                
                throw ex;
                
            }
            
        }
        
    }

    protected void overflowToDisk() {

        TransientBufferStrategy tmp = (TransientBufferStrategy)buf;
        
        int segmentId = 0;
        
        File file = null; // request a unique filename.
        
        /*
         * Set the initial extent to be large enough for the root blocks plus
         * twice the data in the in-memory buffer.
         */
        long initialExtent = FileMetadata.headerSize0 + tmp.getUserExtent() * 2;
        
        final boolean create = true;
        
        final boolean readOnly = false;
        
        // Do not force writes since the store is not restart safe.
        final ForceEnum forceWrites = ForceEnum.No;

        /* Create a unique store file and setup the root blocks.  The file
         * will be pre-extended to the requested initialExtent.
         */
        FileMetadata fileMetadata = new FileMetadata(segmentId, file,
                BufferMode.Disk, useDirectBuffers, initialExtent,
                create, readOnly, forceWrites);
        
        // Mark the file for deletion on exit.
        fileMetadata.file.deleteOnExit();
        
        // Open the disk-based store file.
        DiskOnlyStrategy diskBuf = new DiskOnlyStrategy(Bytes.gigabyte * 2,
                fileMetadata);

        try {

            /*
             * Transfer the data from the in-memory buffer to the disk.  The
             * write begins immediately after the file header.
             */
            
            // setup the transfer source.
            ByteBuffer b = tmp.directBuffer;
            b.limit(tmp.nextOffset);
            b.position(0);
            
            // write the data on the channel.
            diskBuf.channel.write(b,diskBuf.headerSize);
            
            // increment the offset.
            diskBuf.nextOffset += tmp.nextOffset;
            
        } catch(IOException ex) {
            
            try {

                diskBuf.close();
                
            } catch (Throwable ex2) {
                
            }
            
            throw new RuntimeException();
            
        }
        
        this.buf = diskBuf;
        
    }
    
}
