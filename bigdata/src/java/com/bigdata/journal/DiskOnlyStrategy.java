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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;

/**
 * Disk-based journal strategy.
 * <p>
 * Writes are buffered in a write cache. The cache is flushed when it would
 * overflow. As a result only large sequential writes are performed on the
 * store. Reads read through the write cache for consistency.
 * 
 * @todo The flush of the write cache could be made asynchronous if we had two
 *       write buffers, but that increases the complexity significantly. It
 *       would have to be synchronous if invoked from {@link #force(boolean)} in
 *       any case.
 * 
 * @todo A separate read cache could be used for hot records, but the B+Tree
 *       implementations already buffer nodes and leaves so this is unlike to
 *       help anything using indices rather than just the {@link IRawStore}
 *       interface. For this reason you can expect performance tests such as
 *       {@link AbstractMRMWTestCase} to under-report the effective read
 *       performance of the store. Tests written at the B+Tree level are a
 *       better guide here.
 * 
 * @todo test verifying that large records are written directly and that the
 *       write cache is properly flush beforehand.
 * 
 * @todo test verifying that the write cache can be disabled.
 * 
 * @todo test verifying that {@link #writeCacheOffset} is restored correctly on
 *       restart (ie., you can continue to append to the store after restart and
 *       the result is valid).
 * 
 * @todo test verifying that the buffer position and limit are updated correctly
 *       by {@link #write(ByteBuffer)} regardless of the code path.
 * 
 * @todo config parameter for buffer size, direct vs heap allocation of the
 *       {@link #writeCache}, and whether or not the {@link #writeCache} is
 *       enabled.
 * 
 * @todo Retro fit the concept of a write cache into the
 *       {@link DirectBufferStrategy} so that we defer writes onto the disk
 *       until (a) a threshold of data has been buffered; or (b)
 *       {@link #force(boolean)} is invoked. Note that the implementation will
 *       be a bit different since the Direct mode is already fully buffered so
 *       we do not need to allocate a separate writeCache.  However, we will
 *       still need to track the {@link #writeCacheOffset} and maintain a
 *       {@link #writeCacheIndex}.
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
    private final File file;
    
    /**
     * The mode used to open that file.
     */
    private final String fileMode;
    
    /**
     * The IO interface for the file - <strong>use
     * {@link #getRandomAccessFile()} rather than this field</strong>.
     */
    /*private*/ /*final*/ RandomAccessFile raf;

    /**
     * The size of the journal header, including MAGIC, version, and both root
     * blocks. This is used as an offset when computing the address of a record
     * in an underlying file and is ignored by buffer modes that are not backed
     * by a file (e.g., transient) or that are memory mapped (since the map is
     * setup to skip over the header)
     */
    private final int headerSize;
    
    /**
     * Extent of the file. This value should be valid since we obtain an
     * exclusive lock on the file when we open it.
     */
    private long extent;

    private long userExtent;

    /**
     * The buffer used to absorb writes that are destined for the disk. Writes
     * are simply appended into this buffer until it would overflow. On
     * overflow, {@link #flushWriteCache()} is invoked to flush the data to the
     * disk (without synchronizing the disk). If a record is too large to fit
     * into this buffer, then the write cache is flushed and the record is
     * written directly on the disk.
     * <p>
     * Note: We must clone the data since the
     * {@link IRawStore#write(ByteBuffer)} contract says that the caller can
     * reuse the buffer once we return. In order minimize heap churn we simply
     * copy the data into {@link #writeCache}, a {@link ByteBuffer} that
     * buffers recently written records. Writes are deferred until the buffer is
     * would overflow and then all buffered are written at once onto the disk.
     * <p>
     * In order to ensure consistency we read through the {@link #writeCache} in
     * {@link #read(long)}. Otherwise a {@link #write(ByteBuffer)} could return
     * and a subsequent read on the record while it is in the
     * {@link #writeCache} would "miss" causing us to read through to the disk
     * (which would not have the correct data).
     * <p>
     * Note: The write cache design assumes an "append only" store. In
     * particular, it assumes that the application data records are written in
     * are purely sequential manner on the end of the file (the root blocks are
     * outside of the application data). Either the write cache must be disabled
     * or a different design must be used if you are using a store where records
     * may be deleted and recycled.
     * <p>
     * The write cache offers a 27% performance gain when compared to the same
     * condition without the write cache as measured by
     * {@link AbstractMRMWTestCase}.
     */
    private ByteBuffer writeCache;
    
    /**
     * An index into the write cache used for read through on the cache. The
     * keys are the addresses that would be used to read the corresponding
     * record. The values are the position in {@link #buffer} where that record
     * is buffered. A cache miss means that you need to read the record from the
     * disk.
     */
    final private Map<Long,Integer> writeCacheIndex;
    
    /**
     * The next offset at which data in the {@link #writeCache} will be written
     * on the disk. The offset is relative to the start of the user data space.
     * Offset zero(0) addresses the first byte after the root blocks.
     * 
     * @see FileMetadata#nextOffset
     * @see AbstractBufferStrategy#nextOffset
     */
    private long writeCacheOffset;
    
    /**
     * Writes the {@link #writeCache} through to the disk and its position is
     * reset to zero.
     * <p>
     * The caller MUST be synchronized on <i>this</i>.
     */
    void flushWriteCache() {
        
        if(writeCache==null) return;
        
        // #of bytes to write on the disk.
        final int nbytes = writeCache.position();
        
        if(nbytes==0) return;

        // limit := position; position := 0;
        writeCache.flip();
        
        // write the data on the disk file.
        writeOnDisk(writeCache,writeCacheOffset);

        // position := 0; limit := capacity.
        writeCache.clear();
        
        // clear the index since all records were flushed to disk.
        writeCacheIndex.clear();

    }
    
    final public int getHeaderSize() {
        
        return headerSize;
        
    }
    
    final public File getFile() {
        
        return file;
        
    }

    final public RandomAccessFile getRandomAccessFile() {

        return raf;
        
    }

    final public FileChannel getChannel() {
        
        return getRandomAccessFile().getChannel();
        
    }
    
    /**
     * 
     * @param maximumExtent
     * @param fileMetadata
     * @param writeCacheCapacity
     *            The #of bytes that can be buffered by the write cache and also
     *            the maximum length of a sequential write operation when
     *            flushing the write cache to disk. Large sequential writes far
     *            outperform a set of smaller writes since the alliviate the
     *            need to repeatedly seek on the disk.
     */
    DiskOnlyStrategy(long maximumExtent, FileMetadata fileMetadata, int writeCacheCapacity) {

        super(fileMetadata.extent, maximumExtent, fileMetadata.offsetBits,
                fileMetadata.nextOffset, BufferMode.Disk);

        this.file = fileMetadata.file;

        this.fileMode = fileMetadata.fileMode;
        
        this.raf = fileMetadata.raf;

        this.extent = fileMetadata.extent;

        this.headerSize = FileMetadata.headerSize0;
        
        this.userExtent = extent - headerSize;

        if (writeCacheCapacity > 0) {

            // Enable the write cache.

            log.info("Enabling writeCache: capacity="+writeCacheCapacity);

            /*
             * An estimate of the #of records that might fit within the write
             * cache. This is based on an assumption that the "average" record
             * is 4k. This is used solely to assign the initial capacity to the
             * writeCacheIndex.
             */
            final int indexDefaultCapacity = writeCacheCapacity / (4
                    * Bytes.kilobyte32);

            // allocate the write cache : @todo make using a direct buffer an option.
            this.writeCache = ByteBuffer.allocate(writeCacheCapacity);

            // allocate and initialize the write cache index.
            writeCacheIndex = new ConcurrentHashMap<Long, Integer>(indexDefaultCapacity);

        } else {

            // Disable the write cache.
            
            this.writeCache = null;

            writeCacheIndex = null;

        }

        // the offset at which the next record would be written on the file.
        writeCacheOffset = fileMetadata.nextOffset;
        
    }
    
    final public boolean isStable() {
        
        return true;
        
    }

    public boolean isFullyBuffered() {
        
        return false;
        
    }
    
    /**
     * {@link #flushWriteCache() flushs} the {@link #writeCache} before syncing
     * the disk.
     */
    public void force(boolean metadata) {

        // flush all pending writes to disk.
        flushWriteCache();

        try {

            // sync the disk.
            getChannel().force(metadata);

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
        
    }

    /**
     * Closes the file immediately (without flushing any pending writes).
     */
    public void close() {

        /*
         * Note: this clears the [open] flag. It is important to do this first
         * so that we do not re-open the channel once it has been closed.
         */

        super.close();

        // Release the write cache.
        writeCache = null;
        
        try {

            raf.close();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    public void delete() {
        
        if (isOpen()) {

            throw new IllegalStateException();

        }
        
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

    /**
     * Note: {@link ClosedChannelException} and
     * {@link AsynchronousCloseException} can get thrown out of this method
     * (wrapped as {@link RuntimeException}s) if a reader task is interrupted.
     */
    public ByteBuffer read(long addr) {

        if (addr == 0L)
            throw new IllegalArgumentException(ERR_ADDRESS_IS_NULL);

        final long offset = getOffset(addr);

        final int nbytes = getByteCount(addr);

        if (nbytes == 0) {

            throw new IllegalArgumentException(ERR_RECORD_LENGTH_ZERO);

        }

        if (offset + nbytes > nextOffset) {

            throw new IllegalArgumentException(ERR_ADDRESS_NOT_WRITTEN);

        }

        /*
         * Allocate a new buffer of the exact capacity.
         * 
         * Note: we do this even if we are reading from the writeCache since the
         * writeCache may be flush and re-written while the caller is holding
         * onto the returned buffer. If the buffer were a view onto the
         * writeCache, then this would cause the data in returned view to
         * change!
         */

        final ByteBuffer dst = ByteBuffer.allocate(nbytes);

        /*
         * We need to synchronize before we test the write cache since otherwise
         * the cache contents could change asynchronously.
         * 
         * FIXME The synchronization block also covers the case when we have to
         * read through to the disk. Ideally we would not have to remain
         * synchronized for that operation, but inconsistencies have been
         * observed when synchronization is not maintained during the read. I am
         * not sure why this is required, but corrupt data can otherwise be
         * demonstrated by AbstractMRMWTestCase.
         * 
         * Note: this issue appears to be an interaction with the OS or hardware
         * disk cache as the problem is generally demonstrated only after the
         * cache has been given some time to "clear". I have seen this problem
         * using Java 1.5.0_07 (-server -Xms1g -Xmx1g
         * -XX:MaxDirectMemorySize=256M) and Windows/XP service pack 2 but I
         * have not tested on other platforms yet.
         * 
         * Performance is somewhat better if you do not synchronize this block
         * of code. However, the differences are not that extreme. As measured
         * by AbstractMRMWTestCase (timeout=10, nclients=20, percentReaders=.8)
         * the performance is:
         * 
         * write 3.3, read 11.4 mb/s with sychronized(this)
         * 
         * write 3.6, read 13.2 mb/s without sychronized(this)
         * 
         * FIXME Also of interest, the JRockit VM corresponding to 1.5.0_06
         * performs significantly worse on the same test. Check out some other
         * VM and OS versions and see what is going on here!
         */

        synchronized (this) {

            /*
             * Check the write cache for this address. The return value is the
             * position in the writeCache where that record starts and [null] if
             * the record is not in the writeCache (or if the writeCache is
             * disabled).
             */

            final Integer writeCachePosition = writeCacheIndex != null ? writeCacheIndex
                    .get(addr)
                    : null;

            if (writeCachePosition != null) {

                /*
                 * Copy the data into the newly allocated buffer.
                 */

                // the start of the record in writeCache.
                final int pos = writeCachePosition;

                // view onto the writeCache with its own limit and position.
                ByteBuffer tmp = writeCache.asReadOnlyBuffer();

                tmp.limit(pos+nbytes);
                
                tmp.position(pos);
                
                // copy the data into [dst].
                dst.put(tmp);

                // flip buffer for reading.
                dst.flip();
                
                // return the new buffer.
                return dst;

            }

            /*
             * read through to the disk.
             */

            // the offset into the disk file.
            final long pos = offset + headerSize;

            for(int ntries=0; ntries<3; ntries++) {

                if(ntries>0) {

                    /*
                     * Note: clear if we are retrying since the buffer may have
                     * been modified by a partial read.
                     */ 

                    dst.clear();
                    
                }
                
                try {

                    // copy the data into the buffer.
                    final int nread = getChannel().read(dst, pos);

                    if (nread != nbytes) {

                        throw new RuntimeException("Expected to read " + nbytes
                                + " bytes but read " + nread);

                    }

                } catch (ClosedByInterruptException ex) {
                    
                    /*
                     * This indicates that this thread was interrupted. We
                     * always abort in this case.
                     */
                    
                    throw new RuntimeException(ex);

                } catch (AsynchronousCloseException ex) {
                    
                    /*
                     * The channel was closed asynchronously while blocking
                     * during the read. If the buffer strategy still thinks that
                     * it is open then we re-open the channel and re-read.
                     */
                    
                    if(reopenChannel()) continue;
                    
                    throw new RuntimeException(ex);
                    
                } catch (ClosedChannelException ex) {
                    
                    /*
                     * The channel is closed. If the buffer strategy still
                     * thinks that it is open then we re-open the channel and
                     * re-read.
                     */

                    if(reopenChannel()) continue;

                    throw new RuntimeException(ex);
                    
                } catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

            }

            // flip for reading.
            dst.flip();

            // return the buffer.
            return dst;

        } // synchronized(this)

    }

    /**
     * This method transparently re-opens the channel for the backing file.
     * <p>
     * Note: This method is synchronized so that concurrent readers do not try
     * to all open the store at the same time.
     * <p>
     * Note: This method is ONLY invoked by readers. This helps to ensure that a
     * writer that has been interrupted can not regain access to the channel (it
     * does not prevent it, but re-opening for writers is asking for trouble).
     * 
     * @return true iff the channel was re-opened.
     * 
     * @throws IllegalStateException
     *             if the buffer strategy has been closed.
     *             
     * @throws RuntimeException
     *             if the backing file can not be opened (can not be found or
     *             can not acquire a lock).
     */
    synchronized private boolean reopenChannel() {
        
        if(raf.getChannel().isOpen()) {
            
            /* The channel is still open.  If you are allowing concurrent reads
             * on the channel, then this could indicate that two readers each 
             * found the channel closed and that one was able to re-open the
             * channel before the other such that the channel was open again
             * by the time the 2nd reader got here.
             */
            
            return true;
            
        }
        
        if(!isOpen()) {

            // the buffer strategy has been closed.
            
            return false;
            
        }

        try {

            raf = FileMetadata.openFile(file, fileMode, bufferMode);
        
            log.warn("Re-opened file: "+file);
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

        return true;
        
    }
    
    public long write(ByteBuffer data) {

        if (data == null)
            throw new IllegalArgumentException(ERR_BUFFER_NULL);

        // #of bytes to store.
        final int nbytes = data.remaining();

        if (nbytes == 0)
            throw new IllegalArgumentException(ERR_BUFFER_EMPTY);

        final long addr; // address in the store.
        synchronized(this) {
            
            /*
             * The offset at which the record will be written on the disk file
             * (not adjusted for the root blocks).
             */
            final long offset = nextOffset;
            
            // formulate the address that can be used to recover that record.
            addr = toAddr(nbytes, offset);

            if (writeCache != null) {

                /*
                 * Flush the writeCache if the record would cause it to
                 * overflow.
                 */

                if (nbytes + writeCache.position() > writeCache.capacity()) {

                    flushWriteCache();

                }

                /*
                 * This record is to big for the write cache so we write the
                 * record directly on the disk.
                 */

                if (nbytes > writeCache.capacity()) {

                    writeOnDisk(data,nextOffset);

                } else {

                    /*
                     * Queue up the write in the writeCache.
                     */

                    // the position() at which the record is cached.
                    final int position = writeCache.position();

                    // copy the record into the cache.
                    writeCache.put(data);

                    // add the record to the write cache index for read(addr).
                    writeCacheIndex.put(new Long(addr), new Integer(position));

                }
                
            } else {

                /*
                 * The writeCache is disabled so just write the record directly
                 * on the disk.
                 * 
                 * Note: for this case we might be able to move the write
                 * outside of the synchronized() block IFF we also cloned the
                 * data (since the caller is allowed to modify the buffer as
                 * soon as write() returns).
                 * 
                 * Note: We update the writeCacheOffset even when the writeCache
                 * is disabled just to keep it consistent. This allows for the
                 * possibility that the writeCache could be enabled and disabled
                 * at will.
                 */
                
                writeOnDisk(data,nextOffset);

            }
            
            /*
             * Whether we wrote the record on the cache or on the disk, we now
             * increment the offset of the next address to be assigned by the
             * #of bytes in the record.
             */
            
            nextOffset += nbytes;
            
        } // synchronized
                
        return addr;

    }

    /**
     * Write the data on the disk (synchronous).
     * <p>
     * Note: This updates {@link #writeCacheOffset} as well.
     * 
     * @param data
     *            The data. The bytes from the current
     *            {@link ByteBuffer#position()} to the
     *            {@link ByteBuffer#limit()} will be written and the
     *            {@link ByteBuffer#position()} will be advanced to the
     *            {@link ByteBuffer#limit()} . The caller may subsequently
     *            modify the contents of the buffer without side effects (i.e.,
     *            the data are copied onto the disk).
     * @param offset
     *            The offset in the file at which the data will be written.
     */
    void writeOnDisk(ByteBuffer data, long offset) {

        final int nbytes = data.limit();
        
        /* 
         * The position in the file at which the record will be written
         * (this is adjusted for the root blocks).
         */

        final long pos = offset + headerSize;

        final long needed = (offset + nbytes) - userExtent;

        if (needed > 0) {

            if (!overflow(needed)) {

                throw new OverflowException();

            }

        }

        try {

            /*
             * Write the data onto the channel.
             */

            // writes bytes from position to limit on the channel at pos.
            final int count = getChannel().write(data, pos);

            if (count != nbytes) {

                throw new IOException("Expecting to write " + nbytes
                        + " bytes, but wrote " + count + " bytes.");

            }

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        // update the next offset at which data will be written on the disk.
        writeCacheOffset += nbytes;

    }
    
    public void writeRootBlock(IRootBlockView rootBlock,ForceEnum forceOnCommit) {

        if (rootBlock == null)
            throw new IllegalArgumentException();
        
        try {

            FileChannel channel = getChannel();

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

    synchronized public void truncate(long newExtent) {

        long newUserExtent =  newExtent - headerSize;
        
        if (newUserExtent < getNextOffset() ) {
           
            throw new IllegalArgumentException(ERR_TRUNCATE);
            
        }

        if(newUserExtent == getUserExtent()) {
            
            // NOP.
            return;
            
        }
        
        try {

            // extend the file.
            getRandomAccessFile().setLength(newExtent);
            
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

    synchronized public long transferTo(RandomAccessFile out)
            throws IOException {
        
        return super.transferFromDiskTo(this, out);
        
    }
    
}
