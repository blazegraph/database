package com.bigdata.rwstore.sector;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.bigdata.counters.CounterSet;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ForceEnum;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.util.ChecksumUtility;

/**
 * A buffer implementation backed by an {@link IMemoryManager}.
 * 
 * @author <a href="mailto:matyncutcher@users.sourceforge.net">Martyn Cutcher</a>
 * @version $Id$
 */
public class MemStrategy implements IBufferStrategy {
	
	final private IMemoryManager m_mmgr;
	final private IAddressManager m_am;
	
	private volatile boolean m_modifiable = true;
	private volatile boolean m_open = true;
	
	private volatile boolean m_dirty = true;
	
	private volatile IRootBlockView m_rb0 = null;
	private volatile IRootBlockView m_rb1 = null;
	
	public MemStrategy(final IMemoryManager mmgr) {
        
	    if (mmgr == null)
            throw new IllegalArgumentException();
		
	    m_mmgr = mmgr;
		
	    m_am = new IAddressManager() {

			@Override
			public int getByteCount(long addr) {
				return (int) (0xFFFF & addr);
			}

			@Override
			public long getOffset(long addr) {
				return addr >> 32;
			}

			@Override
			public long getPhysicalAddress(final long addr) {
				// Should this compute based on sector index and sector size?
				final int saddr = (int) getOffset(addr);
				final int sector = SectorAllocator.getSectorIndex(saddr);
				final int soffset = SectorAllocator.getSectorOffset(saddr);
				final long paddr = m_mmgr.getSectorSize();
				
				return (paddr * sector) + soffset;
			}

			@Override
			public long toAddr(int nbytes, long offset) {
				return (offset << 32) + nbytes;
			}

			@Override
			public String toString(final long addr) {
				return "PhysicalAddress: " + getPhysicalAddress(addr) + ", length: " + getByteCount(addr);
			}
			
		};
		
		// initialise RootBlocks
		final UUID uuid = UUID.randomUUID(); // Journal's UUID.
		final long createTime = System.currentTimeMillis();
		final ChecksumUtility checker = new ChecksumUtility();
		m_rb0 = new RootBlockView(true, 0,
	            0, 0, 0, 0, 0, 0,
	            uuid,
	            0, 0, 0,
	            StoreTypeEnum.RW,
	            createTime, 0, RootBlockView.currentVersion, checker);

		m_rb1 = new RootBlockView(false, 0,
	            0, 0, 0, 0, 0, 0,
	            uuid,
	            0, 0, 0,
	            StoreTypeEnum.RW,
	            createTime, 0, RootBlockView.currentVersion, checker);	
	}
	
	public IMemoryManager getMemoryManager() {
		return m_mmgr;
	}

	@Override
	public void abort() {
		// NOP
	}

	@Override
	public void closeForWrites() {
		m_modifiable = false;
	}

	@Override
	public void commit(IJournal journal) {
		m_mmgr.commit();
		m_dirty = false;
	}

	@Override
	public IAddressManager getAddressManager() {
		return m_am;
	}

	@Override
	public BufferMode getBufferMode() {
		return BufferMode.MemStore;
	}

	@Override
	synchronized public CounterSet getCounters() {

		if (root == null) {

			root = new CounterSet();

		}

		return root;

	}

	private CounterSet root;

	@Override
	public long getExtent() {
		// return the amount of currently reserved memory
		final long ssze = m_mmgr.getSectorSize();

		return ssze * m_mmgr.getSectorCount();
	}

	@Override
	public int getHeaderSize() {
		// No header
		return 0;
	}

	@Override
	public long getInitialExtent() {
		return m_mmgr.getSectorSize();
	}

	@Override
	public int getMaxRecordSize() {
		return (int) getMaximumExtent();
	}

	@Override
	public long getMaximumExtent() {
		final long ssze = m_mmgr.getSectorSize();

		return ssze * m_mmgr.getMaxSectors();
	}

	@Override
	public long getMetaBitsAddr() {
		// NOP
		return 0;
	}

	@Override
	public long getMetaStartAddr() {
		// NOP
		return 0;
	}

	@Override
	public long getNextOffset() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getOffsetBits() {
		return 0;
	}

	@Override
	public long getUserExtent() {
		return 0;
	}

	@Override
	public ByteBuffer readRootBlock(boolean rootBlock0) {
		return (rootBlock0 ? m_rb0 : m_rb1).asReadOnlyBuffer();
	}

	@Override
	public boolean requiresCommit(IRootBlockView block) {
        return m_dirty;
	}

	@Override
	public long transferTo(RandomAccessFile out) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void truncate(long extent) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean useChecksums() {
		return false;
	}

	@Override
	public void writeRootBlock(IRootBlockView rootBlock,
			ForceEnum forceOnCommitEnum) {
		if (rootBlock.isRootBlock0()) {
			m_rb0 = rootBlock;
		} else {
			m_rb1 = rootBlock;
		}
	}

	@Override
	public void close() {
		m_open = false;
		destroy();
	}

	@Override
	public void delete(long addr) {
		if (!m_modifiable) {
			throw new IllegalStateException("The store is not modifiable");
		}
		m_mmgr.free(addr);
		m_dirty = true;
	}

	@Override
	public void deleteResources() {
		m_mmgr.close();
	}

	@Override
	public void destroy() {
		deleteResources();
	}

	@Override
	public void force(boolean metadata) {
		// NOP
	}

	@Override
	public File getFile() {
		return null;
	}

	@Override
	public IResourceMetadata getResourceMetadata() {
		throw new UnsupportedOperationException();
	}

	@Override
	public UUID getUUID() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isFullyBuffered() {
		return true;
	}

	@Override
	public boolean isOpen() {
		return m_open;
	}

	@Override
	public boolean isReadOnly() {
		return !m_modifiable;
	}

	@Override
	public boolean isStable() {
		return false;
	}

	@Override
	public ByteBuffer read(final long addr) {
		return ByteBuffer.wrap(m_mmgr.read(addr));
	}

	@Override
	public long size() {
		return getExtent();
	}

	@Override
	public long write(ByteBuffer data) {
		m_dirty = true;

		return m_mmgr.allocate(data);
	}

	@Override
	public long write(ByteBuffer data, long oldAddr) {
		m_dirty = true;
		
		final long ret =  write(data);
		
		m_mmgr.free(oldAddr);
				
		return ret;
	}

	// AddressManager delagates
	
	@Override
	public int getByteCount(final long addr) {
		return m_am.getByteCount(addr);
	}

	@Override
	public long getOffset(final long addr) {
		return m_am.getOffset(addr);
	}

	@Override
	public long getPhysicalAddress(final long addr) {
		return m_am.getPhysicalAddress(addr);
	}

	@Override
	public long toAddr(final int nbytes, final long offset) {
		return m_am.toAddr(nbytes, offset);
	}

	@Override
	public String toString(long addr) {
		return m_am.toString(addr);
	}

}
