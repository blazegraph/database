/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.journal.ha.HAWriteMessage;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.quorum.Quorum;
import com.bigdata.rawstore.AbstractRawStore;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rwstore.IAllocationContext;
import com.bigdata.rwstore.RWStore;
import com.bigdata.service.AbstractTransactionService;
import com.bigdata.util.ChecksumUtility;

/**
 * The hook that accesses the RWStore to provide read/write services as opposed
 * to the WORM characteristics of the DiskOnlyStrategy AddressManager
 * 
 * The intent behind this approach is to try to manage the protocol
 * differences between the IStore implementation, assumed as a backing
 * service to the CTC ObjectManager, and an IBufferStrategy service for a
 * BigData Journal.
 * 
 * The most fundamental difference is with the root data, with RootBlock
 * including both low-level store data - such as metabits info - and
 * higher level, journal maintained data.
 * 
 * TODO: Rationalise rootBlock access - use rootblock held by RWStore
 * 
 * TODO: Implement use of IByteArraySlice as alternative to ByteBuffer
 * 
 * @author Martyn Cutcher
 */
public class RWStrategy extends AbstractRawStore implements IBufferStrategy, IHABufferStrategy {
    protected static final Logger log = Logger.getLogger(RWStrategy.class);

	final private FileMetadata m_fileMetadata;
	
	final private Quorum<?,?> m_environment;

    /**
     * The backing store impl.
     * 
     * @see #reopen().
     */
	private volatile RWStore m_store = null;
	
	final private FileMetadataView m_fmv = new FileMetadataView();
	
	private volatile boolean m_open = false;
	
	private volatile IRootBlockView m_rb;
	private volatile IRootBlockView m_rb0;
	private volatile IRootBlockView m_rb1;
	
	final ReentrantLock m_commitLock = new ReentrantLock();
	
    /**
     * Access to the transaction manager of any owning Journal, needed by
     * RWStrategy to manager deleted data.
     */
    private AbstractLocalTransactionManager localTransactionManager = null;
    
//	CounterSet m_counters = new CounterSet();

	/**
	 * It is important to ensure that the RWStrategy keeps a check on the physical root blocks and uses
	 * to manage re-opening of the store.
	 * 
	 * @param fileMetadata
	 */
	RWStrategy(final FileMetadata fileMetadata, final Quorum<?,?> quorum) {

		m_fileMetadata = fileMetadata;
		
		m_environment = quorum;
		
		m_rb = fileMetadata.rootBlock;
		
		m_store = new RWStore(m_fmv, false, quorum); // not read-only for now
		m_open = true;
		
		m_rb = getRootBlock(); // ensure values correct from create/reopen
		
		m_rb0 = copyRootBlock(true);
		m_rb1 = copyRootBlock(false);
		
		m_initialExtent = m_fileMetadata.file.length();
	}

    /**
     * @param rb0
     * @return
     */
	IRootBlockView copyRootBlock(boolean rb0) {
		IRootBlockView rbv = new RootBlockView(rb0, m_rb.getOffsetBits(), m_rb.getNextOffset(), m_rb.getFirstCommitTime(), m_rb.getLastCommitTime(),
				m_rb.getCommitCounter(), m_rb.getCommitRecordAddr(), m_rb.getCommitRecordIndexAddr(), m_fileMetadata.rootBlock.getUUID(),
				m_rb.getQuorumToken(),
				m_rb.getMetaStartAddr(), m_rb.getMetaBitsAddr(), StoreTypeEnum.RW, m_fileMetadata.rootBlock.getCreateTime(), m_rb.getCloseTime(),
				s_ckutil);

		return rbv;
	}

	/**
	 * Create a wrapper to circumvent package visibility issues since we'd like
	 * to keep RWStore in a separate package
	 * 
	 * @author mgc
	 * 
	 */
	interface IFileMetadataView {
		IRootBlockView getRootBlock();

		IRootBlockView getRootBlock0();

		IRootBlockView getRootBlock1();

		File getFile();

		RandomAccessFile getRandomAccessFile();

		public IRootBlockView newRootBlockView(boolean rootBlock0, int offsetBits, long nextOffset,
				long firstCommitTime, long lastCommitTime, long commitCounter, long commitRecordAddr,
				long commitRecordIndexAddr, long metaStartAddr, long metaBitsAddr, long closeTime);
	}

	static final ChecksumUtility s_ckutil = new ChecksumUtility();

	public class FileMetadataView implements IFileMetadataView {

		private FileMetadataView() {
		}

		public IRootBlockView getRootBlock() {
			return m_rb;
		}

		public IRootBlockView getRootBlock0() {
			return m_rb0;
		}

		public IRootBlockView getRootBlock1() {
			return m_rb1;
		}

		public FileMetadata getFileMetadata() {
			return m_fileMetadata;
		}

		public File getFile() {
			return m_fileMetadata.file;
		}

		public RandomAccessFile getRandomAccessFile() {
			return m_fileMetadata.raf;
		}

		public IRootBlockView newRootBlockView(boolean rootBlock0, int offsetBits, long nextOffset,
				long firstCommitTime, long lastCommitTime, long commitCounter, long commitRecordAddr,
				long commitRecordIndexAddr, long metaStartAddr, long metaBitsAddr, long closeTime) {

			IRootBlockView rbv = new RootBlockView(rootBlock0, offsetBits, nextOffset, firstCommitTime, lastCommitTime,
					commitCounter, commitRecordAddr, commitRecordIndexAddr, m_fileMetadata.rootBlock.getUUID(), -1 /* FIXME: quorumToken */,
					metaStartAddr, metaBitsAddr, StoreTypeEnum.RW, m_fileMetadata.rootBlock.getCreateTime(), closeTime,
					s_ckutil);

			// writeRootBlock(rbv, ForceEnum.Force); // not sure if this is really needed now!

			return rbv;
		}

		public String getProperty(String name, String defvalue) {
			return m_fileMetadata.getProperty(name, defvalue);
		}
	}

	public ByteBuffer readRootBlock(boolean rootBlock0) {
		checkReopen();
		
		IRootBlockView rbv = rootBlock0 ? m_rb0 : m_rb1;
		
		return rbv.asReadOnlyBuffer();
	}

	public ByteBuffer read(long addr) {
		checkReopen();
		
		int rwaddr = decodeAddr(addr);		
		int sze = decodeSize(addr);

		if (rwaddr == 0L || sze == 0) {
			throw new IllegalArgumentException();
		}
		
		/**
		 * Allocate buffer to include checksum to allow single read
		 * but then return ByteBuffer excluding those bytes
		 */
		byte buf[] = new byte[sze+4]; // 4 bytes for checksum
		m_store.getData(rwaddr, buf);

		return ByteBuffer.wrap(buf, 0, sze);
	}

	public long write(ByteBuffer data) {
		return write(data, null);
	}

	public long write(ByteBuffer data, IAllocationContext context) {
		checkReopen();
		
		if (data == null) {
			throw new IllegalArgumentException();
		}
		
		final int nbytes = data.remaining();
		
		if (nbytes == 0) {
			throw new IllegalArgumentException();
		}
		
		try { /* FIXME [data] is not always backed by an array, the array may not be visible (read-only), the array offset may not be zero, etc.  Try to drive the ByteBuffer into the RWStore.alloc() method instead. */
		    if(data.hasArray()&&data.arrayOffset()!=0)throw new AssertionError();
			final long rwaddr = m_store.alloc(data.array(), nbytes, context);
			data.position(nbytes); // update position to end of buffer
	
			final long retaddr =  encodeAddr(rwaddr, nbytes);

			return retaddr;
		} catch (RuntimeException re) {
			
			re.printStackTrace();
			
			m_needsReopen = true;			
			
			reopen(); // FIXME

			throw re;
		}
	}

	private void checkReopen() {
		if (m_needsReopen) {
			assert false;
			
			if (m_needsReopen) {
				// reopen(); // should be handled by RWStore
				m_needsReopen = false;
			}
		}		
	}

//	public long allocate(int nbytes) {
//		return encodeAddr(m_store.alloc(nbytes), nbytes);
//	}

	private long encodeAddr(long alloc, int nbytes) {
		alloc <<= 32;
		alloc += nbytes;

		return alloc;
	}

	private int decodeAddr(long addr) {
		addr >>= 32;

		return (int) addr;
	}

	private int decodeSize(long addr) {
		return (int) (addr & 0xFFFFFFFF);
	}

	public void delete(long addr) {
		delete(addr, null);
	}

	/**
	 * Must check whether there are existing transactions which may access
	 * this data, and if not free immediately, otherwise defer.
	 */
	public void delete(long addr, IAllocationContext context) {
		
		final int rwaddr = decodeAddr(addr);
		final int sze = decodeSize(addr);
		
		m_store.free(rwaddr, sze, context);
	}
	
	public void detachContext(IAllocationContext context) {
		m_store.detachContext(context);
	}


	public static class RWAddressManager implements IAddressManager {

		public int getByteCount(long addr) {
			return (int) addr & 0xFFFFFF;
		}

		public long getOffset(long addr) {
			return -(addr >> 32);
		}

		public long toAddr(int nbytes, long offset) {
			offset <<= 32;
			
			return offset + nbytes;
		}

		public String toString(long addr) {
	        return "{off="+getOffset(addr)+",len="+getByteCount(addr)+"}";
		}
	}
	IAddressManager m_am = new RWAddressManager();
	
	public IAddressManager getAddressManager() {
		return m_am;
	}

	public void closeForWrites() {
		// TODO Auto-generated method stub
	    throw new UnsupportedOperationException();
	}

	public BufferMode getBufferMode() {
		return BufferMode.DiskRW;
	}

    /**
     * FIXME Define and implement support for counters. The pattern for this
     * method is to always return a new object so it may be attached to various
     * points in hierarchies belonging to the caller.
     */
	public CounterSet getCounters() {
		return new CounterSet();
	}

	public long getExtent() {
		return this.m_fileMetadata.file.length();
	}

	public int getHeaderSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	long m_initialExtent = 0;

	private boolean m_needsReopen = false;
	
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
	/**
	 * TODO: Should this mean the same
	 */
	public long getUserExtent() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long transferTo(RandomAccessFile out) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * This method means more to a WORM than a RW since it assumes an allocation strategy
	 */
    public long ensureMinFree(final long minFree) {

        throw new UnsupportedOperationException();

    }

    public void truncate(long extent) {
		// TODO Auto-generated method stub

	}

    public void writeRootBlock(final IRootBlockView rootBlock,
            final ForceEnum forceOnCommit) {

		if (rootBlock == null)
			throw new IllegalArgumentException();

		try {
			m_store.checkRootBlock(rootBlock);
			
			if (log.isTraceEnabled()) {
				log.trace("Writing new rootblock with commitCounter: " 
						+ rootBlock.getCommitCounter()
						+ ", commitRecordAddr: " + rootBlock.getCommitRecordAddr()
						+ ", commitRecordIndexAddr: " + rootBlock.getCommitRecordIndexAddr());
			}
			
			m_fileMetadata.writeRootBlock(rootBlock, forceOnCommit);
			
			// Current rootBlock is retained
			m_rb = rootBlock;
			if (m_rb.isRootBlock0())
				m_rb0 = m_rb;
			else
				m_rb1 = m_rb;

		}

		catch (IOException ex) {
			m_needsReopen = true;
			
			reopen(); // force immediate reopen
			
			throw new RuntimeException(ex);

		}

	}

	/**
	 * Set close time in rootBlock, and close file
	 */
	public void close() {
		if (m_fileMetadata.raf == null) {
			throw new IllegalStateException();
		}
		try {
			m_open = false;
			
			m_store.close();
			m_fileMetadata.raf.close();
			m_fileMetadata.raf = null;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void deleteResources() {
		if (m_fileMetadata.raf != null && m_fileMetadata.raf.getChannel().isOpen()) {
			throw new IllegalStateException("Backing store is open");
		}
		
		if (m_fileMetadata.file.exists()) {
			try {
				if (!m_fileMetadata.file.delete()) {
					log.warn("Unable to delete file: " + m_fileMetadata.file);
				}
			} catch (SecurityException e) {
				log.warn("Problem deleting file", e);
			}
		}
	}

	public void destroy() {
		m_store.close();

		if (m_fileMetadata.raf != null && m_fileMetadata.raf.getChannel().isOpen()) {
			try {
				m_fileMetadata.raf.close();
			} catch (IOException e) {
				log.warn("Problem with file close", e);
			}
		}
		
		deleteResources();
	}

	public IRootBlockView getRootBlock() {
		return m_fmv.newRootBlockView(! m_rb.isRootBlock0(), m_rb.getOffsetBits(), getNextOffset(), 
				m_rb.getFirstCommitTime(), m_rb.getLastCommitTime(), m_rb.getCommitCounter(), 
				m_rb.getCommitRecordAddr(), m_rb.getCommitRecordIndexAddr(), getMetaStartAddr(), getMetaBitsAddr(), m_rb.getCloseTime() );
		
	}
	
	/**
	 * commit must use a commit lock to synchronize the rootBlock with the commit.
	 * 
	 * Must pass in earliestTxnTime to commitChanges to enable
	 */
	public void commit() {
		m_commitLock.lock();
		try {
			m_store.commitChanges(); // includes a force(false)
		} finally {
			m_commitLock.unlock();
		}
	}
	
	/**
	 * Calls through to store and then to WriteCacheService.reset
	 */
	public void abort() {
		m_store.checkRootBlock(m_rb);
		
	    m_store.reset();
	}
	
	public void force(boolean metadata) {
		try {
			m_store.flushWrites(metadata);
		} catch (IOException e) {
			m_needsReopen = true;
			
			reopen(); // FIXME
			
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Must ensure that the writeCacheService is reset and direct buffers released.
	 * TODO: Modify so that current WriteCacheService is reset and re-used by new
	 * store.
	 */
	public void reopen() {
		try {
			log.warn("Request to reopen store after interrupt");

			m_store.close();
			m_fileMetadata.raf = new RandomAccessFile(m_fileMetadata.file, m_fileMetadata.fileMode);
			m_store = new RWStore(m_fmv, false, m_environment); // never read-only for now
			m_needsReopen = false;
			m_open = true;
		} catch (Throwable t) {
			t.printStackTrace();
			
			throw new RuntimeException(t);
		}		
	}

	public File getFile() {
		return m_fileMetadata.file;
	}

	public Object getRandomAccessFile() {
		checkReopen();
		
		return m_fileMetadata.raf;		
	}
	public IResourceMetadata getResourceMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	public UUID getUUID() {
		return m_fileMetadata.rootBlock.getUUID();
	}

	public boolean isFullyBuffered() {
		return false;
	}

	public boolean isOpen() {
		// return m_fileMetadata.raf != null && m_fileMetadata.raf.getChannel().isOpen();
		return m_open;
	}

	public boolean isReadOnly() {
		return false;
	}

	public boolean isStable() {
		return true;
	}

	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getByteCount(long addr) {
		return (int) addr & 0xFFFFFFFF;
	}

	public long getOffset(long addr) {
		return addr >> 32;
	}

	public void packAddr(DataOutput out, long addr) throws IOException {
		// TODO Auto-generated method stub

	}

	public long toAddr(int nbytes, long offset) {
		return (offset << 32) + nbytes;
	}

	public String toString(long addr) {
		// TODO Auto-generated method stub
		return null;
	}

	public long unpackAddr(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * The state of the provided block is not relevant since it does not hold
	 * information on recent allocations (the meta allocations will only effect the
	 * root block after a commit)
	 */
	public boolean requiresCommit(IRootBlockView block) {
		return m_store.requiresCommit();
	}

	public long getMetaBitsAddr() {
		return m_store.getMetaBitsAddr();
	}

	public long getMetaStartAddr() {
		return m_store.getMetaStartAddr();
	}

	/**
	 * Appears only to be used in unit tests.  Return current max fix allocation block of 8K.
	 * 
	 * FIXME: need to provide configurable allocation block sizes for the RWStore and this should access the same
	 * information.
	 */
	public int getMaxRecordSize() {
		return 8 * 1024;
	}

	/**
	 * Although the RW Store uses a latched addressing strategy it is not meaningful to make this available 
	 * in this interface.
	 */
	public int getOffsetBits() {
		return 0;
	}
	
	/**
	 * Used for unit tests, could also be used to access raw statistics.
	 * 
	 * @return the associated RWStore
	 */
	public RWStore getRWStore() {
		return m_store;
	}

    // FIXME writeRawBuffer
    public void writeRawBuffer(HAWriteMessage msg, ByteBuffer b)
            throws IOException, InterruptedException {

        throw new UnsupportedOperationException();

    }

    // FIXME readFromLocalStore
    public ByteBuffer readFromLocalStore(long addr) throws InterruptedException {
        
        throw new UnsupportedOperationException();
        
    }

	/**
	 * Called from HAGlue.receiveAndReplicate to ensure the correct file extent
	 * prior to any writes.
	 * For RW this is essential as the allocaiton blocks for current committed data
	 * could otherwise be overwritten and the store invalidated.
	 * 
	 * @see com.bigdata.journal.IHABufferStrategy#setExtentForLocalStore(long)
	 */
	public void setExtentForLocalStore(long extent) throws IOException, InterruptedException {
        
        m_store.establishHAExtent(extent);
        
	}

	public void setTransactionManager(AbstractLocalTransactionManager localTransactionManager) {
		this.localTransactionManager = localTransactionManager;
		m_store.setTransactionService((JournalTransactionService) localTransactionManager.getTransactionService());
	}

	public long getPhysicalAddress(long addr) {
		int rwaddr = decodeAddr(addr);		
		
		return m_store.physicalAddress(rwaddr);
	}

	/**
	 * Saves the current list of delete blocks, returning the address allocated.
	 * This can be used later to retrieve the addresses of allocations to be
	 * freed.
	 * 
	 * @return the address of the delete blocks, or zero if none
	 */
	public long saveDeleteBlocks() {
		return m_store.saveDeferrals();
	}

	public void setCommitRecordIndex(CommitRecordIndex commitRecordIndex) {
		m_store.setCommitRecordIndex(commitRecordIndex);
	}

}
