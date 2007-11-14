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
/*
 * Created on Feb 15, 2007
 */

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.bigdata.rawstore.AbstractRawWormStore;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IMRMW;
import com.bigdata.rawstore.IMROW;
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
 * is {@link #close() closed}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo add configuration properties.
 * 
 * FIXME Use the {@link DiskOnlyStrategy} here but add an option for lazy
 * creation of the file on the disk, disable forcing of writes or force on
 * commit, and mark the file as "temporary". This will let us write in memory
 * until the write cache overflows and then it will start putting down the data
 * on the disk. This has all of the advantages of the current approach (low
 * latency on startup), plus we get MRMW for the temp store.
 */
public class TemporaryRawStore extends AbstractRawWormStore implements IRawStore, IMROW {

    protected static final Logger log = Logger.getLogger(TemporaryRawStore.class);
    
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

    /**
     * Closes the store if it gets GCd.
     */
    protected void finalize() throws Throwable {
        
        try {
            if(open) close();
        } catch (Throwable t) {
            t.printStackTrace(System.err);
        }
        
        super.finalize();
        
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

    /**
     * Note: This operation is a NOP since {@link #close()} always deletes the
     * backing file and {@link #delete()} requires that the store is closed as a
     * pre-condition.
     */
    public void delete() {
        
        if(open) throw new IllegalStateException();
        
        /*
         * NOP
         */
        
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

    /**
     * FIXME readers and writers are being serialized since in order to ensure
     * that {@link #overflowToDisk()} is invoked during a time when no reader
     * and no other writer is trying to access the store. Examine ways to
     * increase the concurrency here. Both the transient and the disk-only modes
     * are fully {@link IMRMW}. It is only the transition that requires us to
     * serialize access.  Also note that the write task can wait when it needs
     * to {@link #overflowToDisk()} since readers can continue to read against
     * the transient buffer (as long as the write task eventually runs).
     */
    synchronized public ByteBuffer read(long addr) {

        if(!open) throw new IllegalStateException();

        return buf.read(addr);
        
    }

    /**
     * FIXME This method is <code>synchronized</code> so that overflow of the
     * buffer from memory to disk will be atomic. While this provides both
     * {@link IMROW} and {@link IMRMW} guarentees for the
     * {@link TemporaryRawStore} it does so at the expense of serializing calls
     * to {@link #write(ByteBuffer)}.
     * <p>
     * Explore options for greater concurrency here in support of concurrent
     * tasks executing against the same transaction or map/reduce jobs writing
     * on a temporary store from multiple map tasks.
     */
    synchronized public long write(ByteBuffer data) {
        
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

    /**
     * Spills the in-memory buffer onto the disk and replaces the
     * {@link TransientBufferStrategy} with a {@link DiskOnlyStrategy}.
     * <p>
     * Note: The TemporaryRawStore transitions from a transient store (fully
     * buffered, so interrupts can not cause the channel to be closed) to a
     * disk-only store (already knows how to handle interrupts).
     * <p>
     * The only place where interrupts could be a problem is during a transition
     * from transient to disk-only, i.e., in this method. However, we already
     * force readers and writers to synchronize for an overflow event so there
     * will only be the one writer running when this method is invoked.
     */
    private void overflowToDisk() {

        log.info("TemporaryRawStore: overflow to disk; nbytes="
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
                fileMetadata, Bytes.megabyte32 /*writeCacheCapacity*/);

        try {

            /*
             * Transfer the data from the in-memory buffer to the disk.  The
             * write begins immediately after the file header.
             */
            
            // setup the transfer source.
            ByteBuffer b = tmp.getBuffer();
            b.limit((int)tmp.nextOffset);
            b.position(0);
            
            diskBuf.writeOnDisk(b, 0L);
            
//            // write the data on the channel.
//            diskBuf.getChannel().write(b,diskBuf.getHeaderSize());
            
            // increment the offset.
            diskBuf.nextOffset += tmp.nextOffset;
            
        } catch(Throwable t) {
            
            try {

                diskBuf.close();
                
            } catch (Throwable ex2) {
                
            }
            
            throw new RuntimeException(t);
            
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
