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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.RootBlockException;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.rawstore.AbstractRawStore;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.StoreManager;
import com.bigdata.service.Event;
import com.bigdata.service.EventResource;
import com.bigdata.service.EventType;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ResourceService;

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
     * The mode that will be used to open the {@link #file} .
     */
    protected static final String mode = "r"; 
    
    /**
     * The file containing the index segment.
     */
    protected final File file;

    /**
     * For reporting via {@link #getResourceMetadata()}.
     */
    final private SegmentMetadata segmentMetadata;
    
    /**
     * Used to correct decode region-based addresses. The
     * {@link IndexSegmentBuilder} encodes region-based addresses using
     * {@link IndexSegmentRegion}. Those addresses are then transparently
     * decoded by this class. The {@link IndexSegment} itself knows nothing
     * about this entire slight of hand.
     * <p>
     * Note: Don't deallocate. It is small and holds useful metadata such as the
     * #of index entries that we would always like to have on hand.
     */
    private final IndexSegmentAddressManager addressManager;
    
    /**
     * An optional <strong>direct</strong> {@link ByteBuffer} containing a disk
     * image of the nodes in the {@link IndexSegment}.
     * <p>
     * Note: This buffer is acquired from the {@link DirectBufferPool} and MUST
     * be released back to that pool.
     * <p>
     * Note: While some nodes will be held in memory by the hard reference queue
     * the use of this buffer means that reading a node that has fallen off of
     * the queue does not require any IO.
     */
    private volatile ByteBuffer buf_nodes;
    
    /**
     * The random access file used to read the index segment. This is
     * transparently re-opened if closed by an interrupt during an NIO
     * operation.
     * <p>
     * A shared {@link FileLock} is requested. If the platform and the volume
     * either DO NOT support {@link FileLock} or support <em>shared</em>
     * {@link FileLock}s then you will be able to open the same
     * {@link IndexSegmentStore} in multiple applications. However, if the
     * platform does not support shared locks then the lock request is converted
     * (by Java) into an exclusive {@link FileLock} and you will not be able to
     * open the {@link IndexSegmentStore} in more than one application at a
     * time.
     * <p>
     * Note: A shared {@link FileLock} makes it impossible to delete an
     * {@link IndexSegmentStore} that is in use. {@link FileLock}s are
     * automatically released when the {@link FileChannel} is closed or the
     * application dies. Using an advisory lock is NOT a good idea as it can
     * leave lock files in place which make it impossible to restart a data
     * service after an abnormal termination. For that reason it is better to
     * NOT use advisory locks on platforms and volumes which do not support
     * {@link FileLock}.
     * 
     * @see #reopenChannel()
     */
    private volatile RandomAccessFile raf;

    /**
     * 
     * @see #raf
     */
    
    /**
     * A read-only view of the checkpoint record for the index segment.
     * <p>
     * Note: Don't deallocate. It is small and holds useful metadata such as the
     * #of index entries that we would always like to have on hand.
     */
    private final IndexSegmentCheckpoint checkpoint;

    /**
     * The metadata record for the index segment.
     * <p>
     * Note: Don't deallocate. Relatively small and it holds some important
     * metadata. By reading this during the ctor we do not have to force the
     * entire index segment to be loaded just to access the index metadata.
     */
    private final IndexMetadata indexMetadata;

    /**
     * Counters specific to the {@link IndexSegmentStore}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class IndexSegmentStoreCounters {

        /**
         * The #of times the store was (re-)opened.
         */
        long openCount;

        /**
         * The #of times the store was closed.
         */
        long closeCount;

        /**
         * A read on a node (whether or not it is buffered).
         */
        long nodesRead;

        /**
         * A read on a node when the nodes are not buffered.
         */
        long nodesReadFromDisk;

        /**
         * A read on a leaf (always reads through to the disk).
         */
        long leavesReadFromDisk;

    }

    /**
     * Counters specific to the {@link IndexSegmentStore}.
     */
    private final IndexSegmentStoreCounters counters = new IndexSegmentStoreCounters();
    
//    final protected void assertOpen() {
//
//        if (!open) {
//            
//            throw new IllegalStateException();
//            
//        }
//
//    }
    
    /**
     * Used to correct decode region-based addresses. The
     * {@link IndexSegmentBuilder} encodes region-based addresses using
     * {@link IndexSegmentRegion}. Those addresses are then transparently
     * decoded by this class. The {@link IndexSegment} itself knows nothing
     * about this entire slight of hand.
     */
    protected final IndexSegmentAddressManager getAddressManager() {
        
        return addressManager;
        
    }
    
    /**
     * A read-only view of the checkpoint record for the index segment.
     */
    public final IndexSegmentCheckpoint getCheckpoint() {
        
        return checkpoint;
        
    }

    /**
     * The {@link IndexMetadata} record for the {@link IndexSegment}.
     * <p>
     * Note: The {@link IndexMetadata#getPartitionMetadata()} always reports
     * that {@link LocalPartitionMetadata#getResources()} is <code>null</code>.
     * This is because the {@link BTree} on the {@link AbstractJournal} defines
     * the index partition view and each {@link IndexSegment} generally
     * participates in MANY views - one per commit point on each
     * {@link AbstractJournal} where the {@link IndexSegment} is part of an
     * index partition view.
     */
    public final IndexMetadata getIndexMetadata() {
        
        return indexMetadata;
        
    }

    /**
     * True iff the store is open.
     */
    private volatile boolean open = false;

    /**
     * Optional.  When defined, {@link Event}s are reported out.
     */
    protected final IBigdataFederation fed;
    private volatile Event openCloseEvent;
    
    /**
     * Open a read-only store containing an {@link IndexSegment}, but does not
     * load the {@link IndexSegment} from the store.
     * <p>
     * Note: If an exception is thrown then the backing file will be closed.
     * <p>
     * Note: Normally access to {@link IndexSegmentStore}s is mediated by the
     * {@link StoreManager} which imposes a canonicalizing weak value cache to
     * ensure that we do not double-open an {@link IndexSegmentStore}.
     * 
     * @param file
     *            The file
     * 
     * @see #loadIndexSegment()
     * 
     * @throws RuntimeException
     *             if there is a problem.
     * @throws RootBlockException
     *             if the root block is invalid.
     */
    public IndexSegmentStore(final File file) {

        this(file, null/* fed */);
        
    }

    /**
     * Ctor variant that accepts an {@link IBigdataFederation} reference and
     * will report out {@link Event}s.
     * 
     * @param file
     * @param fed
     */
    public IndexSegmentStore(final File file, final IBigdataFederation fed) {

        if (file == null)
            throw new IllegalArgumentException();

        this.file = file;

        // MAY be null.
        this.fed = fed;
        
        /*
         * Mark as open so that we can use reopenChannel() and read(long addr)
         * to read other data (the root node/leaf).
         */
        this.open = true;

        try {

            // open the file.
            reopenChannel();

            // read the checkpoint record from the file.
            this.checkpoint = new IndexSegmentCheckpoint(raf);

            // for reporting via getResourceMetadata and toString().
            this.segmentMetadata = new SegmentMetadata(file,
                    checkpoint.segmentUUID, checkpoint.commitTime);
            
            // handles transparent decoding of offsets within regions.
            this.addressManager = new IndexSegmentAddressManager(checkpoint);

            // Read the metadata record.
            this.indexMetadata = readMetadata();

        } catch (IOException ex) {

            _close();

            throw new RuntimeException(ex);

        }

        if (log.isInfoEnabled())
            log.info(checkpoint.toString());
        
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

            if(log.isInfoEnabled())
                log.info("Closing IndexSegmentStore: " + getFile());

            _close();
            
        }
        
    }

    public String toString() {

        /*
         * Note: Only depends on final fields.
         */

        return file.toString();
        
    }

    public IResourceMetadata getResourceMetadata() {

        /*
         * Note: Only depends on final fields.
         */

        return segmentMetadata;
        
    }
    
    /**
     * Re-open a (possibly closed) store. This operation should succeed if the
     * backing file is still accessible.
     * <p>
     * Note: If an exception is thrown then the backing file will be closed.
     * 
     * @throws RootBlockException
     *             if the root block is invalid.
     * @throws RuntimeException
     *             if there is a problem, including a
     *             {@link FileNotFoundException}.
     * 
     * @see #close()
     */
    public void reopen() {

        lock.lock();
        try {

            if (open) {
                /*
                 * The store was already open by the time we got the lock.
                 * 
                 * Note: IndexSegment#readNodeOrLeaf() does not have a lock
                 * before it invokes this method so the backing store can easily
                 * have been concurrently re-opened once that thread gains the
                 * lock.
                 */
//                throw new IllegalStateException("Already open.");
                return;
            }

            try {

                /*
                 * Mark as open so that we can use read(long addr) to read other
                 * data (the root node/leaf).
                 */
                this.open = true;

                // open the file channel for the 1st time.
                reopenChannel();
                
                counters.openCount++;
                
                if (fed != null) {

                    openCloseEvent = new Event(fed, new EventResource(
                            indexMetadata, file),
                            EventType.IndexSegmentStoreOpenClose).start();
                    
                }

            } catch (Throwable t) {

                // clean up.
                _close();

                // re-throw the exception.
                throw new RuntimeException(
                        "Could not (re-) open: file=" + file, t);

            }

        } finally {

            lock.unlock();

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
     * <p>
     * Note: Normally access to {@link IndexSegment}s is mediated by the
     * {@link StoreManager} which imposes a canonicalizing weak value cache to
     * ensure that we do not double-open an {@link IndexSegment}.
     * 
     * @param store
     *            The store.
     * 
     * @return The {@link IndexSegment} or derived class loaded from that store.
     */
    public IndexSegment loadIndexSegment() {

        /*
         * This is grabbed before we request the lock in an attempt to close a
         * possible concurrency window where the finalizer on the index segment
         * might run while we are acquiring the lock. By grabbing a hard
         * reference here we ensure that the finalizer will not run while we are
         * acquiring the lock.  Who knows if this will ever make a difference.
         */
        IndexSegment seg = ref == null ? null : ref.get();

        lock.lock();
        try {

            /*
             * If we did not get the hard reference above then we need to try
             * again now that we have the lock.
             */
            seg = seg != null ? seg : ref == null ? null : ref.get();

            if (seg != null) {

                // ensure "open".
                seg.reopen();

                // return seg.
                return seg;
                
            } else {

                try {

                    final Class cl = Class.forName(indexMetadata
                            .getBTreeClassName());

                    @SuppressWarnings("unchecked")
                    final Constructor ctor = cl
                            .getConstructor(new Class[] { IndexSegmentStore.class });

                    seg = (IndexSegment) ctor
                            .newInstance(new Object[] { this });

                    /*
                     * Attach the counters maintained by AbstractBTree to those
                     * reported for the IndexSegmentStore.
                     * 
                     * Note: These counters are only allocated when the
                     * IndexSegment object is created and this is where we
                     * enforce a 1:1 correspondence between an IndexSegmentStore
                     * and the IndexSegment loaded from that store. However, the
                     * index can be closed and re-opened so we still need to
                     * replace any counters which we find during attach().
                     */

                    getCounters().attach(seg.getBtreeCounters().getCounters(),
                            true/*replace*/);

                    // set the canonicalizing weak reference to the open seg.
                    ref = new WeakReference<IndexSegment>(seg);

                    // return seg.
                    return seg;
                    
                } catch (Exception ex) {

                    throw new RuntimeException(ex);

                }

            }

        } finally {

            lock.unlock();

        }

    }
    /**
     * A canonicalizing weak reference for the {@link IndexSegment} that can be
     * loaded from this store.
     */
    private volatile WeakReference<IndexSegment> ref = null;

    /**
     * A lock used to make open and close operations atomic.
     */
    protected final ReentrantLock lock = new ReentrantLock();
    
    final public boolean isOpen() {
        
        return open;
        
    }
   
    final public boolean isReadOnly() {

        return true;
        
    }
    
    final public boolean isStable() {
        
        return true;
        
    }

    /**
     * Return <code>false</code> since the leaves are not fully buffered even
     * if the nodes are fully buffered.
     */
    final public boolean isFullyBuffered() {
        
        return false;
        
    }
    
    /**
     * Return <code>true</code> if the nodes of the {@link IndexSegment} are
     * fully buffered in memory. The result is consistent as of the time that
     * this method examines the state of the {@link IndexSegmentStore}.
     */
    public boolean isNodesFullyBuffered() {
        
        lock.lock();
        
        try {

            return isOpen() && buf_nodes != null;

        } finally {
            
            lock.unlock();
            
        }
        
    }
    
    final public File getFile() {
        
        return file;
        
    }
    
    /**
     * Closes the file and releases the internal buffers. This operation will
     * quitely succeed if the {@link IndexSegmentStore} is already closed. This
     * operation may be reversed by {@link #reopen()} as long as the backing
     * file remains available. A read on a closed {@link IndexSegmentStore} will
     * transparently {@link #reopen()} the store as long as the backing file
     * remains available. {@link #destroy()} provides an atomic "close and
     * delete" operation.
     */
    public void close() {

        lock.lock();

        try {
        
            if (log.isInfoEnabled())
                log.info(file.toString());

//          assertOpen();

            if(isOpen()) {

                _close();
                
            }
            
        } finally {
            
            lock.unlock();
            
        }
        
    }
        
    /**
     * Method is safe to invoke whether or not the store is "open" and will
     * always close {@link #raf} (if open), release various buffers, and set
     * {@link #open} to <code>false</code>. All exceptions are trapped, a log
     * message is written, and the exception is NOT re-thrown.
     */
    synchronized private void _close() {

        lock.lock();

        try {

            if (raf != null) {

                try {

                    raf.close();

                } catch (IOException ex) {

                    log.error("Problem closing file: " + file, ex);

                    // ignore exception.

                }

                raf = null;
                
            }

            if (buf_nodes != null) {

                try {

                    // release the buffer back to the pool.
                    DirectBufferPool.INSTANCE.release(buf_nodes);

                } catch (Throwable t) {

                    // log error but continue anyway.
                    log.error(this, t);

                } finally {

                    // clear reference since buffer was released.
                    buf_nodes = null;

                }

            }

            open = false;

            counters.closeCount++;

            if (openCloseEvent != null) {

                try {
                    openCloseEvent.end();
                } catch (Throwable t) {
                    log.error(this, t);
                } finally {
                    openCloseEvent = null;
                }

            }
            
            if (log.isInfoEnabled())
                log.info("Closed: file=" + getFile());

        } finally {

            lock.unlock();

        }
        
    }
    
    public void deleteResources() {
        
        lock.lock();
        try {

            if (open)
                throw new IllegalStateException();

            if (!file.delete()) {

                throw new RuntimeException("Could not delete: "
                        + file.getAbsolutePath());

            }
            
        } finally {

            lock.unlock();

        }

    }

    /**
     * Atomically closes the store (iff open) and then deletes the backing file.
     */
    synchronized public void destroy() {

        lock.lock();

        try {

            if (isOpen()) {

                close();

            }

            deleteResources();

        } finally {

            lock.unlock();
            
        }
        
    }

    final public long write(ByteBuffer data) {

        throw new UnsupportedOperationException();

    }

    final public void force(boolean metadata) {
        
        throw new UnsupportedOperationException();
        
    }
    
    final public long size() {

        return checkpoint.length;
        
    }

    synchronized public CounterSet getCounters() {

        if (counterSet == null) {
        
            counterSet = new CounterSet();
            
            counterSet.addCounter("file", new OneShotInstrument<String>(file
                    .toString()));

            // checkpoint (counters are all oneshot).
            {
                
                final CounterSet tmp = counterSet.makePath("checkpoint");
                
                tmp.addCounter("segment UUID", new OneShotInstrument<String>(
                        checkpoint.segmentUUID.toString()));

                // length in bytes of the file.
                tmp.addCounter("length", new OneShotInstrument<String>(
                        Long.toString(checkpoint.length)));

                tmp.addCounter("#nodes", new OneShotInstrument<String>(
                        Long.toString(checkpoint.nnodes)));

                tmp.addCounter("#leaves", new OneShotInstrument<String>(
                        Long.toString(checkpoint.nleaves)));

                tmp.addCounter("#entries", new OneShotInstrument<String>(
                        Long.toString(checkpoint.nentries)));

                tmp.addCounter("height", new OneShotInstrument<String>(
                        Long.toString(checkpoint.height)));

            }
            
            // metadata (all oneshot).
            {
                
                final CounterSet tmp = counterSet.makePath("metadata");
                
                tmp.addCounter("name", new OneShotInstrument<String>(
                        indexMetadata.getName()));

                tmp.addCounter("index UUID", new OneShotInstrument<String>(
                        indexMetadata.getIndexUUID().toString()));
                
            }
            
            // dynamic counters.
            {

                final CounterSet tmp = counterSet.makePath("store");
                
                tmp.addCounter("nodesBuffered", new Instrument<Boolean>() {
                    protected void sample() {
                        setValue(buf_nodes != null);
                    }
                });
                
                tmp.addCounter("openCount", new Instrument<String>() {
                    protected void sample() {
                        setValue(Long.toString(counters.openCount));
                    }
                });

                tmp.addCounter("closeCount", new Instrument<String>() {
                    protected void sample() {
                        setValue(Long.toString(counters.closeCount));
                    }
                });

                tmp.addCounter("nodesRead", new Instrument<String>() {
                    protected void sample() {
                        setValue(Long.toString(counters.nodesRead));
                    }
                });

                tmp.addCounter("nodeReadFromDisk", new Instrument<String>() {
                    protected void sample() {
                        setValue(Long.toString(counters.nodesReadFromDisk));
                    }
                });

                tmp.addCounter("leavesReadFromDisk", new Instrument<String>() {
                    protected void sample() {
                        setValue(Long.toString(counters.leavesReadFromDisk));
                    }
                });

            }

        }
        
        return counterSet;
        
    }
    private CounterSet counterSet;

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
    public ByteBuffer read(final long addr) {

//        assertOpen();
        
        /*
         * True IFF the starting address lies entirely within the region
         * dedicated to the B+Tree nodes.
         */
        final boolean isNodeAddr = addressManager.isNodeAddr(addr);
        
        if (log.isDebugEnabled()) {

            log.debug("addr=" + addr + "(" + addressManager.toString(addr)
                    + "), isNodeAddr="+isNodeAddr);
            
        }
        
        // abs. offset of the record in the file.
        final long offset = addressManager.getOffset(addr);

        // length of the record.
        final int length = addressManager.getByteCount(addr);

        if (isNodeAddr) {

            // a node.
            
            counters.nodesRead++;
            
            synchronized (this) {

                if (buf_nodes != null) {

                    counters.nodesReadFromDisk++;
                    
                    return readFromBuffer(offset, length);

                }

            }

            // The data need to be read from the file.
            return readFromFile(offset, length);

        } else {

            // A leaf.
            
            counters.leavesReadFromDisk++;

            // The data need to be read from the file.
            return readFromFile(offset, length);

        }

    }

    /**
     * The [addr] addresses a node and the data are buffered. Create and return
     * a read-only view so that concurrent reads do not modify the buffer state.
     * <p>
     * Note: The caller MUST be synchronized on [this] and ensure that the
     * {@link #buf_nodes} is non-<code>null</code> before calling this
     * method. This ensures that the buffer is valid across the call.
     * 
     * @param offset
     * @param length
     * @return
     */
    final private ByteBuffer readFromBuffer(final long offset, final int length) {
        
        /*
         * Note: As long as the state of [buf_nodes] (its position and limit)
         * can not be changed concurrently, we should not need to serialize
         * creations of the view (this is a bit paranoid, but the operation is
         * very lightweight).
         */
        final ByteBuffer tmp;
        synchronized(this) {
            
            tmp = buf_nodes.asReadOnlyBuffer();
            
        }

        // correct the offset so that it is relative to the buffer.
        final long off = offset - checkpoint.offsetNodes;

        // set the limit on the buffer to the end of the record.
        tmp.limit((int)(off + length));

        // set the position on the buffer to the start of the record.
        tmp.position((int)off);

        /*
         * Create a slice of that view showing only the desired record. The
         * position() of the slice will be zero(0) and the limit() will be
         * the #of bytes in the record.
         * 
         * Note: slice restricts the view available to the caller to the
         * view that was setup on the buffer at the moment that the slice
         * was obtained.
         */
        return tmp.slice();
        
    }
    
    /**
     * Read the record from the file.
     */
    final private ByteBuffer readFromFile(final long offset, final int length) {

        try {
            
            // Allocate buffer: limit = capacity; pos = 0.
            final ByteBuffer dst = ByteBuffer.allocate(length);

            // read into [dst] - does not modify the channel's position().
            FileChannelUtility.readAll(opener, dst, offset);
            
            // successful read from file; flip buffer for reading by caller.
            dst.flip();

            // done.
            return dst;
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);
            
        }

    }
    
    private final IReopenChannel opener = new IReopenChannel() {

        public String toString() {
            
            return IndexSegmentStore.this.toString();
            
        }
        
        public FileChannel reopenChannel() throws IOException {

            return IndexSegmentStore.this.reopenChannel();

        }
        
    };
    
    /**
     * This method transparently re-opens the channel for the backing file.
     * <p>
     * Since the {@link IndexSegment} is a read-only data structure, all of the
     * in-memory state remains valid and we only need to re-open the
     * {@link FileChannel} to the backing store and retry. In particular, we do
     * not need to re-read the root node, {@link IndexMetadata},
     * {@link BloomFilter}, etc. All we have to do is re-open the
     * {@link FileChannel}.
     * <p>
     * Note: This method is synchronized so that concurrent readers do not try
     * to all open the store at the same time. Further, this is the only method
     * other than {@link #_close()} that can set {@link #raf}. Since both this
     * method and {@link #_close()} are synchronized the state of that field is
     * well known inside of this method.
     * <p>
     * Note: {@link OverlappingFileLockException}s can arise when there are
     * concurrent requests to obtain a shared lock on the same file. Personally,
     * I think that this is a bug since the lock requests are shared and should
     * be processed without deadlock. However, the code handles this case by
     * proceeding without the lock - exactly as it would handle the case where a
     * shared lock was not available. This is still somewhat fragile since it
     * someone does not test the {@link FileLock} and was in fact granted an
     * exclusive lock when they requested a shared lock then this code will be
     * unwilling to send the resource. There are two ways to make that work out -
     * either we DO NOT use {@link FileLock} for read-only files (index
     * segments) or we ALWAYS discard the {@link FileLock} if it is not shared
     * when we requested a shared lock and proceed without a lock. For this
     * reason, the behavior of this class and {@link ResourceService} MUST
     * match.
     * 
     * @see ResourceService
     * @see http://blogs.sun.com/DaveB/entry/new_improved_in_java_se1
     * @see http://forums.sun.com/thread.jspa?threadID=5324314.
     * 
     * @return The {@link FileChannel}.
     * 
     * @throws IOException
     *             if the backing file can not be locked.
     */
    final synchronized private FileChannel reopenChannel() throws IOException {

        /*
         * Note: closing an IndexSegmentStore DOES NOT prevent it from being
         * transparently reopened.
         */
//      assertOpen();
        if(!open) {
            
            reopen();
            
        }
        
        if (raf != null && raf.getChannel().isOpen()) {
            
            /*
             * The channel is still open. If you are allowing concurrent reads
             * on the channel, then this could indicate that two readers each
             * found the channel closed and that one was able to re-open the
             * channel before the other such that the channel was open again by
             * the time the 2nd reader got here.
             */
            
            return raf.getChannel();
            
        }
        
        // open the file.
        this.raf = new RandomAccessFile(file, mode);

        if (log.isInfoEnabled())
            log.info("(Re-)opened file: " + file);

        try {

            /*
             * Request a shared file lock.
             */
            final FileLock fileLock = raf.getChannel().tryLock(0,
                    Long.MAX_VALUE, true/* shared */);
            
            if (fileLock == null) {

                /*
                 * Note: A null return indicates that someone else holds the
                 * lock. This can happen if the platform does not support shared
                 * locks or if someone requested an exclusive file lock.
                 */

                try {
                    raf.close();
                } catch (Throwable t) {
                    // ignore.
                }

                throw new IOException("File already locked: file=" + getFile());

            }

            if(!fileLock.isShared()) {
                
                /*
                 * DO NOT hold an exclusive lock for an index segment store
                 * file!
                 * 
                 * Note: On platforms where shared locks are not support the JDK
                 * will escalate to an exclusive lock. That would interfere with
                 * our ability to MOVE index segments around using the
                 * ResourceService so we make sure that we don't hold an
                 * exclusive file lock here.
                 */
                
                fileLock.release();
                
            }
            
        } catch (OverlappingFileLockException ex) {

            /*
             * Note: OverlappingFileLockException can be thrown when there are
             * concurrent requests to obtain the same shared lock. I consider
             * this a JDK bug. It should be possible to service both requests
             * without deadlock.
             * 
             * Note: I had seen this exception occasionally even before we
             * started using the ResourceService to MOVE index segments around.
             * That also looks like a JDK bug since we only request the FileLock
             * in this method, we know that the channel is closed, and this
             * method is [synchronized]. Ergo, it should not be possible to have
             * overlapping requests (concurrent requests).
             */

            if (log.isInfoEnabled())
                log
                        .info("Will proceed without lock: file=" + file + " : "
                                + ex);

        } catch (IOException ex) {

            /*
             * Note: This is true of NFS volumes. This is Ok and should be
             * ignored. However the backing file is not protected against
             * accidental deletes or overwrites.
             */

            if (log.isInfoEnabled())
                log.info("FileLock not supported: file=" + getFile(), ex);

        }

        return raf.getChannel();
        
    }

    /**
     * Attempts to read the index nodes into {@link #buf_nodes}.
     * <p>
     * Note: If the nodes could not be buffered then reads against the nodes
     * will read through to the backing file.
     */
    protected void bufferIndexNodes() throws IOException {
        
        if(!lock.isHeldByCurrentThread()) {

            throw new IllegalMonitorStateException();
            
        }
        
        if (buf_nodes != null) {

            // already buffered.
            return;
            
        }
        
        if(checkpoint.nnodes == 0) {
        
            throw new IllegalStateException();
            
        }

        if(checkpoint.offsetNodes == 0L) {
            
            throw new IllegalStateException();
            
        }

        if(checkpoint.extentNodes > DirectBufferPool.INSTANCE.getBufferCapacity()) {
            
            /*
             * The buffer would be too small to contain the nodes.
             */
            
            log.warn("Node extent exceeds buffer capacity: extent="
                    + checkpoint.extentNodes + ", bufferCapacity="
                    + DirectBufferPool.INSTANCE.getBufferCapacity());
            
            return;
            
        }

        /*
         * This code is designed to be robust. If anything goes wrong then we
         * make certain that the direct buffer is released back to the pool, log
         * any errors, and return to the caller. While the nodes will not be
         * buffered if there is an error throw in this section, if the backing
         * file is Ok then they can still be read directly from the backing
         * file.
         */
        try {

            /*
             * Attempt to allocate a buffer to hold the disk image of the nodes.
             * 
             * FIXME There should be a direct buffer pool instance specifically
             * configured to buffer the index segment nodes. This will make it
             * possible to buffer the nodes even when the buffer size required
             * is not a good match for the buffer size used as the write cache
             * for the journal. We need to report counters for all buffer pools
             * in order to accurately track the memory overhead for each
             * purpose. [make sure to replace all references to the default
             * INSTANCE with the specialized pool and make sure that we have the
             * chance to configure the pool before it is placed into service.]
             */
            
            buf_nodes = DirectBufferPool.INSTANCE.acquire(100/* ms */,
                    TimeUnit.MILLISECONDS);
            
            if (log.isInfoEnabled())
                log.info("Buffering nodes: #nodes=" + checkpoint.nnodes
                        + ", #bytes=" + checkpoint.extentNodes + ", file=" + file);

            // #of bytes to read.
            buf_nodes.limit((int)checkpoint.extentNodes);
            
            // attempt to read the nodes into the buffer.
            FileChannelUtility.readAll(opener, buf_nodes,
                    checkpoint.offsetNodes);
            
            buf_nodes.flip();
            
        } catch (Throwable t1) {

            /*
             * If we could not obtain a buffer without blocking, or if there was
             * ANY problem reading the data into the buffer, then release the
             * buffer and return. The nodes will not be buffered, but if the
             * file is Ok then the index will simply read through to the disk
             * for the nodes.
             */

            if (buf_nodes != null) {

                try {
                
                    // release buffer back to the pool.
                    DirectBufferPool.INSTANCE.release(buf_nodes);
                    
                } catch (Throwable t) {
                    
                    // log error and continue.
                    log.error(this, t);
                    
                } finally {
                    
                    // make sure the reference is cleared.
                    buf_nodes = null;
                    
                }
                
            }

            // log error and continue.
            log.error(this, t1);

        }

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
        
        if (log.isInfoEnabled())
            log.info("reading bloom filter: "+addressManager.toString(addr));
        
        final long off = addressManager.getOffset(addr);
        
        final int len = addressManager.getByteCount(addr);
        
        final ByteBuffer buf = ByteBuffer.allocate(len);

        buf.limit(len);

        buf.position(0);

        try {

            // read into [dst] - does not modify the channel's position().
            FileChannelUtility.readAll(opener, buf, off);
            
            buf.flip(); // Flip buffer for reading.
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        assert buf.position() == 0;
        assert buf.limit() == len;

        final BloomFilter bloomFilter = (BloomFilter) SerializerUtil.deserialize(buf);

        if (log.isInfoEnabled())
            log.info("Read bloom filter: bytesOnDisk=" + len );

        return bloomFilter;

    }

    /**
     * Reads the {@link IndexMetadata} record directly from the file (this is
     * invoked by the ctor).
     */
    final private IndexMetadata readMetadata() throws IOException {

        final long addr = checkpoint.addrMetadata;
        
        assert addr != 0L;
        
        if (log.isInfoEnabled())
            log.info("reading metadata: "+addressManager.toString(addr));
        
        final long off = addressManager.getOffset(addr);
        
        final int len = addressManager.getByteCount(addr);
        
        final ByteBuffer buf = ByteBuffer.allocate(len);

        buf.limit(len);

        buf.position(0);

        try {

            // read into [dst] - does not modify the channel's position().
            FileChannelUtility.readAll(opener, buf, off);
            
            buf.flip(); // Flip buffer for reading.
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        assert buf.position() == 0;
        assert buf.limit() == len;

        final IndexMetadata md = (IndexMetadata) SerializerUtil
                .deserialize(buf);

        if (log.isInfoEnabled())
            log.info("Read metadata: " + md);

        return md;

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
