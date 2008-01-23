/*

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
 * Created on Nov 26, 2007
 */

package com.bigdata.journal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.IResourceTransfer;
import com.bigdata.service.IWritePipeline;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * FIXME I've outlined a re-design below.
 * 
 * <pre>
    - Media redundency

      - derive a constrained interface similar to the Channel for
        read/write.  The main additional constraint is that
        implementations should be able to take advantage of the
        append-only nature of the store for writes and perhaps have a
        custom extension for updating the root blocks, since that is
        the only non-append write.
      
        - provide a FileChannel implementation.  it just writes on the
          file channel.

        - provide a write cache implementation.  it should be based on
          the disk only strategy's write cache and provide
          asynchronous write behind to the delegate.  this is the only
          layer that should buffer writes and it is the only layer
          that should handle read through of its write cache.  in
          order to have asynchronous writes there should be multiple
          buffers.  only one buffer is actively recieving writes while
          the other buffers are providing a read-behind cache (until
          they are reused) and an asynchronous write-behind to the
          delegate.

    - provide a network socket implementation (client and
          server). synchronous read/write on a network socket.  only
          the write facet is required for media redundency.  the
          read+write design would allow us to write on a remote
          service.  the client should have a single thread handling
          writes on the socket. the server should be single threaded
          using NIO.

    - provide a tee (writes on two delegates, reads from one).
          this will be used to write on both a local FileChannel and a
          remote store via a network socket.  it could also be used to
          write on two FileChannels for live redundency of the
          journal.

    - provide a memory only implementation based on the Transient
          buffer strategy.

    - The stacked set would look like this when used for media
          redundency.

      IRawStore>WriteCache>Tee>(NetworkChannel+FileChannel)

      Alternative stacks would handle a purely transient model
      with media redundency.

      IRawStore>WriteCache>Tee>(NetworkChannel+MemoryChannel)

      or without

      IRawStore>MemoryChannel
      
      
   There must also be support for replication of the secondary resources for
   an index partition (the index segments).
 * </pre>
 * 
 * A class that intercepts writes on an {@link IRawStore} and causes the written
 * record to be replicated on one or more remote store(s). Writes are replicated
 * asynchronously to minimize latency for the caller.
 * <p>
 * {@link #force(boolean)} will not return until the writes have been forced to
 * each of the remote store(s). This means that read committed requests may be
 * directed to either the local store or to any remote store that replicates
 * this local store. However, uncommitted writes are NOT guarenteed to be
 * available for reads at the remote stores until the next
 * {@link #force(boolean)}.
 * <p>
 * This class is responsible for streaming writes to the primary remote store,
 * which is the first remote store in an ordered list of remote stores. The
 * primary is then responsible for replicating those writes to the secondary
 * remote store (if any). In turn, the secondary remote store will replicate the
 * write to its downstream remote store (if any), and so on.
 * <p>
 * If the primary remote store fails or is taken out of service, then this class
 * will promote the secondary remote store to become the new primary remote
 * store and will ensure that the secondary remote store is up to date no later
 * than the next {@link #force(boolean)}.
 * <p>
 * If the number of replicated instances of the local store falls below a target
 * replication count then a new remote store will be discovered and added to the
 * replication chain.
 * 
 * FIXME IRawStore is not the best way to integrate this. We need to integrate
 * at a level that exposes the [nextOffset], write(), commit(), force(),
 * abort(), close(), and closeAndDelete() notices.
 * <p>
 * Also, since we are not sending along addresses paired with individual records
 * we need the ability on the replicated store to write directly on the backing
 * file or buffer rather than on any write cache.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see IWritePipeline
 * @see IResourceTransfer
 */
public class ReplicatedStore implements IRawStore {

    protected static final Logger log = Logger.getLogger(ReplicatedStore.class);
    
    /**
     * The local store -- all operations are delegated to this object.
     */
    private final IRawStore localStore;

    /**
     * Interface exposed by a store that will be maintained as a replica of
     * another store.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IRemoteStore {
        
        /**
         * Returns the next offset at which data would be written on the remote
         * store. This may be used to learn how much data has been transferred
         * to the remote store. This is useful mainly when the primary fails and
         * you need to potentially send along some data to the secondary which
         * would otherwise be lost.
         */
        public long nextOffset();
        
        /**
         * Append the data to the remote store.
         * 
         * @param data
         *            The data
         * @param offset
         *            The expected value of {@link #nextOffset()}. This is used
         *            to verify that the remote store will write the data at the
         *            expected location.
         * @param commit
         *            When <code>true</code> the write request must be
         *            processed synchronously by the remote store. Whether or
         *            not the remote store forces its data to stable storage on
         *            commit is a function of how that store was provisioned.
         *            See {@link Options#FORCE_ON_COMMIT}.
         * 
         * @throws IllegalStateException
         *             if the offsets do not agree.
         * 
         * @todo we do not necessarily need to force writes to stable storage
         *       when using replicated storage since replication will handle
         *       failover and provides a better guarentee of durability than
         *       just knowing that the data is on a disk on the local machine.
         *       by avoiding sync to disk alltogether we can potentially
         *       increase the overall throughput.
         */
        public void write(byte[] data, long offset, boolean commit);

        /**
         * Directs the replica to advance the offset at which the next byte
         * would be written. This is used when an abort() leaves data written on
         * the local store that is not made restart safe and which can therefore
         * never be accessed. Rather than copying the data, we just inform the
         * replica that it can (a) ignore any bytes to be written before the
         * specified offset; and (b) that it must advance its next offset to the
         * specified value.
         * 
         * @todo in order to use this method we have to first interrupt any
         *       pending data transfer task(s).
         * 
         * @param nextOffset
         * 
         * @throws IllegalStateException
         *             if the new value for [nextOffset] is less than the
         *             current value.
         */
        public void abort(long nextOffset);
        
        /**
         * Close the remote store (invoked when the local store is closed since
         * there will be no futher data transfers).
         */
        public void close();
        
        /**
         * The name of the backing file on which the replicated data is being
         * written -or- <code>null</code> iff the replicated store is not
         * backed by stable media (ie, if it is transient).
         */
        public File getFile();
        
    }
    
    /**
     * The primary replica of the local store on which this class writes.
     */
    private IRemoteStore replicaStore;
    
    /**
     * The next offset at which data will be written on the
     * {@link #replicaStore}.
     */
    private long replicaNextOffset;
    
    /**
     * Service writes on the {@link #replicaStore}.
     */
    private final ExecutorService writeService = Executors
            .newSingleThreadExecutor(DaemonThreadFactory.defaultThreadFactory());
    
    /**
     * Used to coordinate asynchronous incremental writes.
     */
    private final Lock lock = new ReentrantLock();
    
    /**
     * Condition is signaled when a data transfer to the primary remote store is
     * complete.
     */
    private final Condition written = lock.newCondition();

    /**
     * The #of active + pending data transfer requests.
     */
    private final AtomicInteger writing = new AtomicInteger(0);
    
    /**
     * Constructor wraps a local store such that it will be replicated on
     * another store.
     * 
     * @param localStore
     *            The local store.
     */
    public ReplicatedStore(IRawStore localStore) {
        
        if (localStore == null)
            throw new IllegalArgumentException();

        this.localStore = localStore;
        
    }
    
    /**
     * Task transfers data to the primary remote store.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class WriteTask implements Runnable {

        final boolean commit;
        
        public WriteTask(boolean commit) {
            
            this.commit = commit;
            
            writing.incrementAndGet();
            
        }
        
        /**
         * Transfers all data between the <em>from</em> offset of the last
         * byte recorded on the remote store and the <em>to</em> offset last
         * byte recorded on the local store to the remote store. Both
         * <em>from</em> and <em>to</em> are fixed when this method starts
         * such that concurrent writes appending more data to the local store
         * will not be transferred by a given invocation of this method.
         * 
         * FIXME we need to monitor for exceptions in this method. data transfer
         * problems generally indicate a dead replica or a network problem
         * reaching the replica. the application should be able to continue
         * merrily regardless of those problems.
         * 
         * @todo in the edge case when the last replica has failed we need to
         *       make sure that {@link ReplicatedStore#force(boolean)} will
         *       still return in a timely manner since primaryNextOffset will
         *       never reach getNextOffset() - at least not for the current
         *       replica.
         *       <p>
         *       This also points out that we need to use locks so that we can
         *       switch out the downstream replica safely.
         */
        public void run() {

            try {

                final long to = getNextOffset();

                long from = replicaNextOffset;

                assert from >= 0;

                assert to >= from;

                while (from < to) {

                    final int nbytes = (int) Math
                            .min(to - from, MAX_WRITE_SIZE);

                    final long offset = replicaNextOffset;

                    final long addr = toAddr(nbytes, offset);

                    log.info("Reading " + nbytes + " bytes from offset="
                            + offset);

                    ByteBuffer tmp = read(addr);

                    replicaStore.write(tmp.array(), offset, commit);

                    replicaNextOffset += nbytes;

                    log.info("Wrote " + nbytes + " on remote store at offset="
                            + offset);

                }

            } finally {

                writing.decrementAndGet();

            }
            
        }

    }
    
    /**
     * The minimum #of bytes that will be transferred during an incremental
     * write on the {@link #replicaStore}.
     */
    private final int MIN_WRITE_SIZE = 10 * Bytes.kilobyte32;
    
    /**
     * The maximum #of bytes that will be transferred at once. This is used to
     * avoid very large reads on the local store since we have to buffer the
     * data until it has been written onto the {@link #replicaStore}.
     */
    private final int MAX_WRITE_SIZE = 10 * Bytes.megabyte32;

    /**
     * Extended to trigger an asynchronous data transfer once the minimum amount
     * of data has been buffered.
     */
    public long write(ByteBuffer data) {

        // write on the local store.
        final long addr = localStore.write(data);

        /*
         * Synchronize so that we do not start up multiple asynchronous data
         * transfers at once.
         */
        
        synchronized(this) {
        
            // the offset of the next byte to be written on the local store.
            final long localNextOffset = getNextOffset();

            // #of bytes not yet on the remote store.
            long delta = localNextOffset - replicaNextOffset;

            assert delta >= 0;

            if (delta > MIN_WRITE_SIZE && writing.get() == 0) {

                /*
                 * Request an incremental data transfer that will bring the
                 * remote store up to date with the state of the local store as
                 * of the moment that this request was created.
                 */

                writeService
                        .submit(new WriteTask(false/* commit */));

            }

        }
        
        return addr;
        
    }

    public long getNextOffset() {

        throw new UnsupportedOperationException();
        
    }
    
    public void force(boolean metadata) {
       
        /* 
         * Force the local store to stable media.
         */        
        localStore.force(metadata);
        
        /*
         * Submit task that will bring the downstream replicas up to date and
         * then wait for that task to complete.
         */
        
        lock.lock();
        
        try {
        
            final long localNextOffset = getNextOffset();
            
            writeService.submit(new WriteTask(true/* commit */));
            
            do {
                
                if(!written.await(200,TimeUnit.MILLISECONDS)) {
                    
                    log.info("Still awaiting downstream replicas");
                    
                    continue;

                }

            } while (replicaNextOffset != localNextOffset);
        
        } catch (InterruptedException e) {
            
            throw new RuntimeException(
                    "Interrupted awaiting downstream replicas: " + e, e);
            
        } finally {
            
            lock.unlock();
            
        }
        
    }

    /**
     * Terminate the {@link #writeService}.
     */
    private void shutdown() {
        
        try {

            writeService.shutdownNow();
            
            if(!writeService.awaitTermination(2, TimeUnit.SECONDS)) {
                
                log.warn("Timeout awaiting termination.");
                
            }
            
        } catch (InterruptedException e) {
            
            log.warn("Interrupted awaiting termination.",e);
            
        } finally {
            
            try {

                replicaStore.close();
                
            } catch (Throwable t) {
                
                log.error("Problem closing the downstream replicas", t);
                
            }
            
        }
        
    }
    
    public void close() {

        shutdown();
        
        localStore.close();
        
    }

    public void closeAndDelete() {

        shutdown();

        localStore.closeAndDelete();
        
    }

    public void delete() {
        localStore.delete();
    }

    public Object deserialize(byte[] b, int off, int len) {
        return localStore.deserialize(b, off, len);
    }

    public Object deserialize(byte[] b) {
        return localStore.deserialize(b);
    }

    public Object deserialize(ByteBuffer buf) {
        return localStore.deserialize(buf);
    }

    public int getByteCount(long addr) {
        return localStore.getByteCount(addr);
    }

    public File getFile() {
        return localStore.getFile();
    }

    public long getOffset(long addr) {
        return localStore.getOffset(addr);
    }

    public boolean isFullyBuffered() {
        return localStore.isFullyBuffered();
    }

    public boolean isOpen() {
        return localStore.isOpen();
    }

    public boolean isStable() {
        return localStore.isStable();
    }

    public void packAddr(DataOutput out, long addr) throws IOException {
        localStore.packAddr(out, addr);
    }

    public ByteBuffer read(long addr) {
        return localStore.read(addr);
    }

    public byte[] serialize(Object obj) {
        return localStore.serialize(obj);
    }

    public long size() {
        return localStore.size();
    }

    public long toAddr(int nbytes, long offset) {
        return localStore.toAddr(nbytes, offset);
    }

    public String toString(long addr) {
        return localStore.toString(addr);
    }

    public long unpackAddr(DataInput in) throws IOException {
        return localStore.unpackAddr(in);
    }

}
