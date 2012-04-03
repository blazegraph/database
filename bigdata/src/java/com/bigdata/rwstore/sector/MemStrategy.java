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
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.IAddressManager;

public class MemStrategy implements IBufferStrategy {
	
	final IMemoryManager m_mmgr;
	final IAddressManager m_am;
	
	boolean m_modifiable = true;
	boolean m_open = true;
	
	public MemStrategy(IMemoryManager mmgr) {
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
				// TODO Auto-generated method stub
				return "PhysicalAddress: " + getPhysicalAddress(addr) + ", length: " + getByteCount(addr);
			}
			
		};
	}
	
	public IMemoryManager getMemoryManager() {
		return m_mmgr;
	}

	@Override
	public void abort() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void closeForWrites() {
		m_modifiable = false;
	}

	@Override
	public void commit(IJournal journal) {
		// NOP
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
	public CounterSet getCounters() {
		// TODO Auto-generated method stub
		return null;
	}

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
		// NOP
		return null;
	}

	@Override
	public boolean requiresCommit(IRootBlockView block) {
		// NOP
		return false;
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
		throw new UnsupportedOperationException();
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
		return m_mmgr.allocate(data);
	}

	@Override
	public long write(ByteBuffer data, long oldAddr) {
		// since there is no commit, we might as well
		// immediately recycle!
		m_mmgr.free(oldAddr);
		
		return write(data);
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
