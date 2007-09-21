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

import com.bigdata.rawstore.AbstractRawWormStore;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.util.ChecksumUtility;

/**
 * A non-restart-safe store for temporary data that buffers data in memory until
 * a {@link #maximumInMemoryExtent} has been reached and then converts to a
 * disk-based store with a maximum capacity determined by the configuration of
 * the {@link WormAddressManager}. On conversion to a disk-backed store, the
 * disk file is created using the temporary file mechansism and is marked for
 * eventual deletion no later than when the JVM exits and as soon as the store
 * is {@link #close()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo The {@link TemporaryRawStore} would benefit from any caching or AIO
 *       solutions developed for the {@link DiskOnlyStrategy}.
 * 
 * @todo The temporary store does NOT currently support MROW since it does not
 *       lock out readers when converting from a {@link TransientBufferStrategy}
 *       to a {@link DiskOnlyStrategy}. At this time, MROW is not required for
 *       the {@link TemporaryRawStore} since it is used by a single-threaded
 *       process such as a transaction and not by multiple threads (unlike the
 *       Journal which does support MROW).
 */
public class TemporaryRawStore extends AbstractRawWormStore implements IRawStore {

    protected final static int DEFAULT_INITIAL_IN_MEMORY_EXTENT = Bytes.megabyte32 * 10;

    protected final static int DEFAULT_MAXIMUM_IN_MEMORY_EXTENT = Bytes.megabyte32 * 100;
    
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
     * Create a {@link TemporaryRawStore} with an initial in-memory capacity of
     * 10M that will grow up to 100M before converting into a disk-based store
     * backed by a temporary file. These defaults are appropriate for a
     * relatively small number of processes that will write a lot of data. If
     * you have a lot of processes then you need to be more conservative with
     * RAM in the initial allocation and switch over to disk sooner. For
     * example, transactions use smaller defaults in order to support a large
     * #of concurrent transactions without a large memory burden.
     * 
     * @todo the memory growth strategy does not respect the in-memory maximum
     *       without parameterizing and overriding the #of bytes to extent the
     *       store on overflow for {@link TransientBufferStrategy}
     */
    public TemporaryRawStore() {

        this(WormAddressManager.DEFAULT_OFFSET_BITS,
                DEFAULT_INITIAL_IN_MEMORY_EXTENT,
                DEFAULT_MAXIMUM_IN_MEMORY_EXTENT, false);
        
    }
    
    /**
     * Create a {@link TemporaryRawStore} with the specified configuration.
     * 
     * @param offsetBits
     *            This determines the capacity of the store file and the maximum
     *            length of a record.  The value is passed through to
     *            {@link WormAddressManager#WormAddressManager(int)}.
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
    public TemporaryRawStore(int offsetBits, long initialInMemoryExtent,
            long maximumInMemoryExtent, boolean useDirectBuffers) {
        
        super(offsetBits);
        
        buf = new TransientBufferStrategy(offsetBits, initialInMemoryExtent,
                maximumInMemoryExtent, useDirectBuffers);
        
        this.initialInMemoryExtent = initialInMemoryExtent;
        
        this.maximumInMemoryExtent = maximumInMemoryExtent;
        
        this.useDirectBuffers = useDirectBuffers;
        
    }

    public File getFile() {
        
        if(!open) throw new IllegalStateException();
        
        return buf.getFile();
        
    }
    
    /**
     * Close the store and delete the associated file, if any.
     */
    public void close() {

        if(!open) throw new IllegalStateException();
        
        open = false;
        
        buf.closeAndDelete();
        
        buf = null;
        
    }

    public void closeAndDelete() {
    
        // Close already deletes the backing file.
        close();
        
    }

    public void force(boolean metadata) {
        
        if(!open) throw new IllegalStateException();
        
        buf.force(metadata);
        
    }

    public long size() {
        
        return buf.size();
        
    }
    
    public boolean isOpen() {
        
        return open;
        
    }

    /**
     * Always returns <code>false</code> since the store will be deleted as soon
     * as it is closed.
     */
    public boolean isStable() {
    
        if(!open) throw new IllegalStateException();
        
        return false;
        
    }
    
    public boolean isFullyBuffered() {
        
        if(!open) throw new IllegalStateException();
        
        return buf.isFullyBuffered(); 
        
    }

    public ByteBuffer read(long addr) {

        if(!open) throw new IllegalStateException();

        return buf.read(addr);
        
    }

    public long write(ByteBuffer data) {
        
        if(!open) throw new IllegalStateException();
        
        try {
            
            /*
             * Note: this operation will transparently extend the in-memory
             * buffer as necessary up to the specified maximum capacity.
             */
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

        System.err.println("TemporaryRawStore: overflow to disk; nbytes="
                + buf.getNextOffset());
        
        TransientBufferStrategy tmp = (TransientBufferStrategy)buf;
        
        File file;
        
        try {

            file = File.createTempFile("bigdata", ".tmpStore");
            
        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        /*
         * Set the initial extent to be large enough for the root blocks plus
         * twice the data in the in-memory buffer.
         */
        final long initialExtent = FileMetadata.headerSize0 + tmp.getUserExtent() * 2;
        
        final long maximumDiskExtent = Bytes.gigabyte32 * 2;
        
        final boolean create = false;
        
        final boolean isEmptyFile = true;
        
        final boolean deleteOnExit = true;
        
        final boolean readOnly = false;
        
        // Do not force writes since the store is not restart safe.
        final ForceEnum forceWrites = ForceEnum.No;

        // The store is never pre-existing so we do not have a checksum to validate.
        final boolean validateChecksum = false;
        
        // We still need an object to compute the checksum to be stored in the root blocks.
        final ChecksumUtility checker = new ChecksumUtility();
        
        /* Create a unique store file and setup the root blocks.  The file
         * will be pre-extended to the requested initialExtent.
         */
        FileMetadata fileMetadata = new FileMetadata(file, BufferMode.Disk,
                useDirectBuffers, initialExtent, maximumDiskExtent, create,
                isEmptyFile, deleteOnExit, readOnly, forceWrites,
                getOffsetBits(), validateChecksum, checker);
        
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
            b.limit((int)tmp.nextOffset);
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
    
    /**
     * The maximum length of a record that may be written on the store.
     */
    final public int getMaxRecordSize() {

        return ((AbstractRawWormStore) buf).getAddressManger()
                .getMaxByteCount();

    }

}
