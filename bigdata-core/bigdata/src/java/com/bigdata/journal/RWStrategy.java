/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.counters.CounterSet;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumRead;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.msg.HARebuildRequest;
import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHARebuildRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.writecache.WriteCacheService;
import com.bigdata.journal.AbstractJournal.ISnapshotData;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumException;
import com.bigdata.rawstore.AbstractRawStore;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IAllocationContext;
import com.bigdata.rawstore.IPSOutputStream;
import com.bigdata.rwstore.IRWStrategy;
import com.bigdata.rwstore.IRawTx;
import com.bigdata.rwstore.RWStore;
import com.bigdata.rwstore.RWStore.StoreCounters;
import com.bigdata.util.ChecksumError;

/**
 * A highly scalable persistent {@link IBufferStrategy} wrapping the
 * {@link RWStore} which may be used as the backing store for a {@link Journal}.
 * <p>
 * The {@link RWStore} manages allocation slots. This can translate into an
 * enormous space savings on the disk for large data sets (when compared to the
 * WORM) since old revisions of B+Tree nodes and leaves may be recycled
 * efficiently.
 * 
 * <h2>History</h2>
 * 
 * The {@link RWStrategy} supports access to historical commit states in
 * combination with the history retention policy of the
 * {@link ITransactionService}.
 * 
 * <h2>Compatibility</h2>
 * 
 * The {@link RWStore} uses a distinct binary layout on the disk based which is
 * not directly compatible with the WORM binary storage layer. The WORM and the
 * {@link RWStore} uses the same file header and root blocks. However, the
 * {@link RWStore} defines some fields in the root blocks which are not used by
 * the WORM store such as the metabits info. In addition, some of the root block
 * fields defined by the WORM store are not used by the {@link RWStore}.
 * 
 * @see RWStore.Options
 * 
 * @author Martyn Cutcher
 */
public class RWStrategy extends AbstractRawStore implements IBufferStrategy,
        IHABufferStrategy, IRWStrategy {

    private static final transient Logger log = Logger.getLogger(RWStrategy.class);

    private final IAddressManager m_am;

    /**
     * The backing store implementation.
     */
	private final RWStore m_store;
	
	/**
	 * The {@link UUID} for the store.
	 */
	private final UUID m_uuid;

	/**
	 * The size of the backing file when it was opened by the constructor.
	 */
    final private long m_initialExtent;

    /**
     * The HA {@link Quorum} (optional).
     */
    private final Quorum<?,?> m_quorum;

	/**
	 * 
	 * @param fileMetadata
	 * @param quorum The HA {@link Quorum} (optional).
	 */
    RWStrategy(final FileMetadata fileMetadata, final Quorum<?, ?> quorum) {

        if (fileMetadata == null)
            throw new IllegalArgumentException();
        
	    m_uuid = fileMetadata.rootBlock.getUUID();
	    
	    // MAY be null.
	    m_quorum = quorum;
	    
		m_store = new RWStore(fileMetadata, quorum); 
		
		m_am = new RWAddressManager(m_store);
		
		m_initialExtent = fileMetadata.file.length();
	
	}

    public ByteBuffer readRootBlock(final boolean rootBlock0) {

        return m_store.readRootBlock(rootBlock0);

	}

    @Override
	public ByteBuffer read(final long addr) {
		
        try {
            // Try reading from the local store.
            return readFromLocalStore(addr);
        } catch (InterruptedException e) {
            // wrap and rethrow.
            throw new RuntimeException(e);
        } catch (ChecksumError e) {
            /*
             * Note: This assumes that the ChecksumError is not wrapped by
             * another exception. If it is, then the ChecksumError would not be
             * caught.
             */
            // log the error.
            try {
                log.error(e + " : addr=" + toString(addr), e);
            } catch (Throwable ignored) {
                // ignore error in logging system.
            }
            // update the performance counters.
            final StoreCounters<?> c = (StoreCounters<?>) m_store.getStoreCounters()
                    .acquire();
            try {
                c.checksumErrorCount++;
            } finally {
                c.release();
            }
            if (m_quorum != null && m_quorum.isHighlyAvailable()) {
                if (m_quorum.isQuorumMet()) {
                    try {
                        // Read on another node in the quorum.
                        final byte[] a = ((QuorumRead<?>) m_quorum.getMember())
                                .readFromQuorum(m_uuid, addr);
                        return ByteBuffer.wrap(a);
                    } catch (Throwable t) {
                        throw new RuntimeException("While handling: " + e, t);
                    }
                }
            }
            // Otherwise rethrow the checksum error.
            throw e;
        }

	}

	public long write(final ByteBuffer data) {
		
	    return write(data, null);
	    
	}

//    @Override
//    public long write(ByteBuffer data, long oldAddr, IAllocationContext context) {
//        return write(data, oldAddr);
//    }

    /**
     * Overridden to integrate with the shadow allocator support of the
     * {@link RWStore}. Shadow allocators may be used to isolate allocation
     * changes (both allocating slots and releasing slots) across different
     * processes.  
     */
	@Override
	public long write(final ByteBuffer data, final IAllocationContext context) {

        if (data == null)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BUFFER_NULL);

        if (data.hasArray() && data.arrayOffset() != 0) {
            /*
             * @todo [data] is not always backed by an array, the array may not
             * be visible (read-only), the array offset may not be zero, etc.
             * Try to drive the ByteBuffer into the RWStore.alloc() method
             * instead.
             * 
             * See https://sourceforge.net/apps/trac/bigdata/ticket/151
             */
            throw new AssertionError();
        }

        final int nbytes = data.remaining();

        if (nbytes == 0)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BUFFER_EMPTY);

        final long rwaddr = m_store.alloc(data.array(), nbytes, context);

        data.position(nbytes); // update position to end of buffer

        final long retaddr = encodeAddr(rwaddr, nbytes);

        return retaddr;

    }

	private long encodeAddr(long alloc, final int nbytes) {
		alloc <<= 32;
		alloc += nbytes;

		return alloc;
	}

	/** Pull the latched address out of the int64 address. */
	private int decodeAddr(long addr) {

	    addr >>= 32;

		return (int) addr;
	}

	private int decodeSize(final long addr) {

	    return (int) (addr & 0xFFFFFFFF);
	    
	}

	public void delete(final long addr) {
    
	    delete(addr, null/* IAllocationContext */);
	    
	}

	/**
	 * Must check whether there are existing transactions which may access
	 * this data, and if not free immediately, otherwise defer.
	 */
	public void delete(final long addr, final IAllocationContext context) {

		final int rwaddr = decodeAddr(addr);
		
		final int sze = decodeSize(addr);

        if (rwaddr == 0L)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_ADDRESS_IS_NULL);

        if (sze == 0)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BAD_RECORD_SIZE);
		
		m_store.free(rwaddr, sze, context);
		
	}
	
    public void detachContext(final IAllocationContext context) {
        
        m_store.detachContext(context);
        
    }

    public void abortContext(final IAllocationContext context) {
        
        m_store.abortContext(context);
        
    }

    /**
     * Operation is not supported.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
	public void closeForWrites() {
		// @todo could be implemented at some point.
	    throw new UnsupportedOperationException();
	}

	public BufferMode getBufferMode() {

	    return BufferMode.DiskRW;
	    
	}

	public CounterSet getCounters() {

	    return m_store.getCounters();
	    
	}

	public long getExtent() {

	    return m_store.getStoreFile().length();
	    
	}

	public int getHeaderSize() {
	 
	    return FileMetadata.headerSize0;
	    
	}

	public long getInitialExtent() {
		
	    return m_initialExtent;
	    
	}

	public long getMaximumExtent() {
		
	    return 0L;
	    
	}

	public boolean useChecksums() {
	    
	    return true;
	    
	}
	
	public long getNextOffset() {
		
	    return m_store.getNextOffset();
	    
	}

	public long getUserExtent() {
	
	    return m_store.getFileStorage();
	    
	}

    /**
     * Operation is not supported.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
	public long transferTo(RandomAccessFile out) throws IOException {
	    
	    // @todo could perhaps be implemented at some point.
	    throw new UnsupportedOperationException();
	    
	}

    public void truncate(final long extent) {
        
        m_store.establishExtent(extent);
        
	}

    public void writeRootBlock(final IRootBlockView rootBlock,
            final ForceEnum forceOnCommit) {

        m_store.writeRootBlock(rootBlock, forceOnCommit);

	}

    private void assertOpen() {

        if (!m_store.isOpen())
            throw new IllegalStateException(AbstractBufferStrategy.ERR_NOT_OPEN);

    }

	public void close() {
	    
	    // throw exception if open per the API.
	    assertOpen();
	    
		m_store.close();
		
	}

    public void deleteResources() {

        if (m_store.isOpen())
            throw new IllegalStateException(AbstractBufferStrategy.ERR_OPEN);

        final File file = m_store.getStoreFile();

        if (file.exists()) {

            if (!file.delete()) {

//                throw new RuntimeException("Unable to delete file: " + file);
                log.warn("Unable to delete file: " + file);

            }

        }

	}

	public void destroy() {

	    // close w/o exception throw.
	    m_store.close();

	    // delete the backing store.
		deleteResources();
		
	}

	@Override
    public void commit() {

        m_store.commit();
        
	}
	
	/**
	 * Calls through to store and then to WriteCacheService.reset
	 */
	@Override
	public void abort() {

	    m_store.reset();
	    
	}
	
    public void force(final boolean metadata) {
        
        try {
        
            m_store.flushWrites(metadata);
        
        } catch (IOException e) {

            throw new RuntimeException(e);
            
        }
        
	}
    
	public File getFile() {

	    return m_store.getStoreFile();
	    
	}
    
	/**
     * Not supported - this is available on the {@link AbstractJournal}.
     * 
     * @throws UnsupportedOperationException
     *             always
     */
    public IResourceMetadata getResourceMetadata() {
        
        throw new UnsupportedOperationException();
        
    }
    
	public UUID getUUID() {
		
	    return m_uuid;
	    
	}

	public boolean isFullyBuffered() {
		
	    return false;
	    
	}

	public boolean isOpen() {

	    return m_store.isOpen();
	    
	}

	public boolean isReadOnly() {
		
	    return false;
	    
	}

	public boolean isStable() {
		
	    return true;
	    
	}

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the #of bytes in the file rather than the user
     * bytes. This is because the {@link RWStore} does not know the #of bytes of
     * user data in each allocation slot. Therefore it is not able to keep
     * accurrate track of the user bytes as allocation slots are cycled.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/212 (Records must
     *      store the as-written length for HA failover reads to be successful.)
     */
	public long size() {

	    return m_store.getFileStorage();
	    
	}

    /*
     * IAddressManager
     */

    public IAddressManager getAddressManager() {
        return m_am;
    }

    public int getByteCount(final long addr) {
        return m_am.getByteCount(addr);
    }

    public long getOffset(final long addr) {
        return m_am.getOffset(addr);
    }

    public long toAddr(final int nbytes, final long offset) {
        return m_am.toAddr(nbytes, offset);
    }

    public String toString(final long addr) {
        return m_am.toString(addr);
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * The state of the provided block is not relevant since it does not hold
     * information on recent allocations (the meta allocations will only effect
     * the root block after a commit). This is passed through to the
     * {@link RWStore} which examines its internal state.
     */
    public boolean requiresCommit(final IRootBlockView block) {

        return m_store.requiresCommit();
        
    }

    /**
     * Supports protocol in BigdataSailConnection to check for modifications
     * prior to calling rollback().
     * 
     * @return true if store has been modified since last commit()
     */
    @Override
	public boolean isDirty() {
		return m_store.requiresCommit();
	}

	public long getMetaBitsAddr() {
        
        return m_store.getMetaBitsAddr();
        
    }

    public long getMetaStartAddr() {

        return m_store.getMetaStartAddr();
        
    }

	public int getMaxRecordSize() {

	    return m_store.getMaxAllocSize() - 4/* checksum */;
	    
	}

    /**
     * Although the RW Store uses a latched addressing strategy it is not
     * meaningful to make this available in this interface.
     */
    public int getOffsetBits() {

        return 0;
        
    }
	
//	/**
//	 * Used for unit tests, could also be used to access raw statistics.
//	 * 
//	 * @return the associated RWStore
//	 */
//    @Deprecated
//	public RWStore getRWStore() {
//		
//	    return m_store;
//	    
//	}

    public RWStore getStore() {

        return m_store;

    }
    
    public long getPhysicalAddress(final long addr) {

        // extract the latched address.
        final int rwaddr = decodeAddr(addr);        
        
        // obtain the byte offset on the file.
        return m_store.physicalAddress(rwaddr);
        
    }

	/*
	 * IHABufferStrategy
	 */

    public void writeRawBuffer(final IHAWriteMessage msg, final IBufferAccess b)
            throws IOException, InterruptedException {

        m_store.writeRawBuffer(msg, b);

    }

    @Override
    public Future<Void> sendHALogBuffer(final IHALogRequest req,
            final IHAWriteMessage msg, final IBufferAccess b)
            throws IOException, InterruptedException {

        return m_store.sendHALogBuffer(req, msg, b);

    }

    @Override
    public Future<Void> sendRawBuffer(final IHARebuildRequest req,
            // long commitCounter, long commitTime,
            final long sequence, final long quorumToken, final long fileExtent,
            final long offset, final int nbytes, final ByteBuffer b)
            throws IOException, InterruptedException {

        return m_store.sendRawBuffer(req, /* commitCounter, commitTime, */
                sequence, quorumToken, fileExtent, offset, nbytes, b);
        
    }
    
    @Override
    public void writeOnStream(final OutputStream os, final ISnapshotData snapshotData,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum, final long token)
            throws IOException, QuorumException {

        try {
			m_store.writeOnStream(os, snapshotData, quorum, token);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

    }

    @Override
    public Object snapshotAllocators() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public void computeDigest(final Object snapshot, final MessageDigest digest)
            throws DigestException, IOException {

        m_store.computeDigest(snapshot, digest);

    }

    public ByteBuffer readFromLocalStore(final long addr)
            throws InterruptedException {

        final int rwaddr = decodeAddr(addr);

        final int sze = decodeSize(addr);

        if (rwaddr == 0L)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_ADDRESS_IS_NULL);

        if (sze == 0)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BAD_RECORD_SIZE);

        /**
         * Allocate buffer to include checksum to allow single read but then
         * return ByteBuffer excluding those bytes
         */
//        final byte buf[] = new byte[sze + 4]; // 4 bytes for checksum
//
//        m_store.getData(rwaddr, buf, 0, sze+4);
//
//        return ByteBuffer.wrap(buf, 0, sze);
        
        return m_store.getData(rwaddr,  sze);

    }

    /**
     * Called from HAGlue.receiveAndReplicate to ensure the correct file extent
     * prior to any writes. For RW this is essential as the allocation blocks
     * for current committed data could otherwise be overwritten and the store
     * invalidated.
     * 
     * @see com.bigdata.journal.IHABufferStrategy#setExtentForLocalStore(long)
     */
    public void setExtentForLocalStore(final long extent) throws IOException,
            InterruptedException {

        m_store.establishExtent(extent);

    }

    /**
     * An assert oriented method that allows a finite number of addresses
     * to be monitored to ensure it is not freed.
     * 
     * @param addr - address to be locked
     * @return true - for use in assert statement
     */
	public boolean lockAddress(final long addr) {
		m_store.lockAddress(decodeAddr(addr));
		
		return true;
	}

//	/**
//	 * If history is retained this returns the time for which
//	 * data was most recently released.  No request can be made for data
//	 * earlier than this.
//	 * @return latest data release time
//	 */
	@Override
	public long getLastReleaseTime() {
		return m_store.getLastReleaseTime();
	}

	/**
	 * Lifted to provide a direct interface from the Journal so that the
	 * CommitRecordIndex can be pruned prior to store commit.
	 */
	public int checkDeferredFrees(final AbstractJournal journal) {
		final int totalFreed = m_store.checkDeferredFrees(/*true,*/ journal); // free now if possible
		
		if (totalFreed > 0 && log.isInfoEnabled()) {
			log.info("Freed " + totalFreed + " deferralls on commit");
		}
		
		return totalFreed;
	}

    /**
     * Return true if the address is marked as committed in the {@link RWStore}
     * in memory bit maps.
     * 
     * @param addr
     *            The address.
     */
	public boolean isCommitted(final long addr) {
		
	    return m_store.isCommitted(decodeAddr(addr));
	    
	}

    /**
     * Return <code>true</code> iff the address was in the write cache as of the
     * moment the write cache was inspected.
     * 
     * @param addr
     *            The address.
     */
    public boolean inWriteCache(final long addr) {

        return m_store.inWriteCache(decodeAddr(addr));
        
    }

	@Override
	public IRawTx newTx() {

		return m_store.newTx();
	}

	//@Martyn:  Please review
	/*
	 * 
	@Override
	public void registerContext(final IAllocationContext context) {
		m_store.registerContext(context);
		
	}
	*/

	@Override
	public void registerExternalCache(
			final ConcurrentWeakValueCache<Long, ICommitter> historicalIndexCache,
			final int byteCount) {
		m_store.registerExternalCache(historicalIndexCache, byteCount);
	}

	@Override
	public long saveDeferrals() {
		return m_store.saveDeferrals();
	}

	@Override
	public InputStream getInputStream(final long addr) {
		return m_store.getInputStream(addr);
	}

	@Override
	public IPSOutputStream getOutputStream() {
		return m_store.getOutputStream();
	}

	@Override
	public IPSOutputStream getOutputStream(final IAllocationContext context) {
		return m_store.getOutputStream(context);
	}

	@Override
	public void resetFromHARootBlock(final IRootBlockView rootBlock) {
		m_store.resetFromHARootBlock(rootBlock);
	}

    @Override
    public long getBlockSequence() {
        return m_store.getBlockSequence();
    }

    @Override
    public long getCurrentBlockSequence() {
        return m_store.getCurrentBlockSequence();
    }

	@Override
	public ByteBuffer readRaw(final long position, final ByteBuffer transfer) {
		return m_store.readRaw(position, transfer);
	}

	@Override
	public void writeRawBuffer(final HARebuildRequest req,
			final IHAWriteMessage msg, final ByteBuffer transfer)
			throws IOException {
		if (req == null)
			throw new IllegalArgumentException();

//		if (m_rebuildSequence != msg.getSequence())
//			throw new IllegalStateException(
//					"Invalid sequence number for rebuild, expected: "
//							+ m_rebuildSequence + ", actual: "
//							+ msg.getSequence());

		m_store.writeRaw(msg.getFirstOffset(), transfer);

		if (log.isDebugEnabled())
			log.debug("Transfer rebuild: " + msg.getSequence() + ", address: "
					+ msg.getFirstOffset());

//		m_rebuildSequence++;
	}

	@Override
	public Lock getCommitLock() {
	    return m_store.getCommitLock();
	}
	
	@Override
	public void postCommit() {
		m_store.postCommit();
	}

	@Override
	public void postHACommit(final IRootBlockView rootBlock) {
		m_store.postHACommit(rootBlock);
	}

	@Override
	public WriteCacheService getWriteCacheService() {
	    return m_store.getWriteCacheService();
	}
	
	@Override
	public StoreState getStoreState() {
		return m_store.getStoreState();
	}

	//@Martyn:  Please review
	@Override
	public IAllocationContext newAllocationContext(boolean isolated) {
		return m_store.newAllocationContext(isolated);
	}
	
//	@Override
//	public boolean isFlushed() {
//		return m_store.isFlushed();
//	}
//	private int m_rebuildSequence = -1;
//	
//	@Override
//	public void prepareForRebuild(final HARebuildRequest req) {
//		m_store.prepareForRebuild(req);
//		m_rebuildSequence = 0;
//	}
//
//	@Override
//	public void completeRebuild(final HARebuildRequest req, final IRootBlockView rbv) {
//		m_store.completeRebuild(req, rbv);
//		m_rebuildSequence = -1;
//	}

}
