package com.bigdata.rwstore;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.rwstore.RWStore.AllocationStats;
import com.bigdata.util.ChecksumUtility;

/**
 * BlobAllocator.
 * 
 * Manages Blob allocations using a list of {@link FixedAllocator}s.
 * 
 * The main advantage of this is for re-allocation, since the
 * {@link FixedAllocator}s can be efficiently re-cycled where a fixed Blob
 * creates issues of best fit and fragmentation.
 * 
 * Some simple patterns would cause un-reallocatable storage, consider a Blob
 * that always re-allocated to a larger size, or a pattern where several blobs
 * got larger together, in these scenarios, smaller allocations would never be
 * re-used, whilst the mechanism of component based allocation is easily
 * re-used.
 * 
 * @author mgc
 */
public class BlobAllocator implements Allocator {
	
    private static final transient Logger log = Logger.getLogger(BlobAllocator.class);

	final private int[] m_hdrs = new int[254];
	final private RWStore m_store;
	private int m_diskAddr;
	private int m_index;
	private int m_sortAddr;
	private ArrayList m_freeList;
	private long m_startAddr;
	// @todo javadoc. why 254?
	private int m_freeSpots = 254;
	
	public BlobAllocator(final RWStore store, final int sortAddr) {
		m_store = store;
		m_sortAddr = sortAddr;
		
		if (log.isInfoEnabled())
			log.info("New BlobAllocator");
	}
	
	public void addAddresses(final ArrayList addrs) {
		// not relevant for BlobAllocators
	}

	public boolean addressInRange(final int addr) {
		// not relevant for BlobAllocators
		return false;
	}

	// @todo javadoc.  Why is this method a NOP (other than the assert).
	public int alloc(final RWStore store, final int size, final IAllocationContext context) {
		assert size > (m_store.m_maxFixedAlloc-4);
		
		return 0;
	}

	// @todo why does this return false on all code paths?
	public boolean free(final int addr, final int sze) {
		if (sze < (m_store.m_maxFixedAlloc-4))
			throw new IllegalArgumentException("Unexpected address size");
		final int alloc = m_store.m_maxFixedAlloc-4;
		final int blcks = (alloc - 1 + sze)/alloc;		
		
		int hdr_idx = (-addr) & RWStore.OFFSET_BITS_MASK;
		if (hdr_idx > m_hdrs.length)
			throw new IllegalArgumentException("free BlobAllocation problem, hdr offset: " + hdr_idx + ", avail:" + m_hdrs.length);
		
		final int hdr_addr = m_hdrs[hdr_idx];
		
		if (hdr_addr == 0) {
			return false;
		}
		
		// read in header block, then free each reference
		final byte[] hdr = new byte[(blcks+1) * 4 + 4]; // add space for checksum
		m_store.getData(hdr_addr, hdr);
		
		final DataInputStream instr = new DataInputStream(
				new ByteArrayInputStream(hdr, 0, hdr.length-4) );
		try {
			final int allocs = instr.readInt();
			for (int i = 0; i < allocs; i++) {
				final int nxt = instr.readInt();
				m_store.free(nxt, m_store.m_maxFixedAlloc);
			}
			m_store.free(hdr_addr, hdr.length);
			m_hdrs[hdr_idx] = 0;
			if (m_freeSpots++ == 0) {
				m_freeList.add(this);
			}
			
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
		
		return false;
	}
	
	public int getFirstFixedForBlob(final int addr, final int sze) {
		if (sze < (m_store.m_maxFixedAlloc-4))
			throw new IllegalArgumentException("Unexpected address size: " + sze);

		final int alloc = m_store.m_maxFixedAlloc-4;
		final int blcks = (alloc - 1 + sze)/alloc;		
		
		final int hdr_idx = (-addr) & RWStore.OFFSET_BITS_MASK;
		if (hdr_idx > m_hdrs.length)
			throw new IllegalArgumentException("free BlobAllocation problem, hdr offset: " + hdr_idx + ", avail:" + m_hdrs.length);
		
		final int hdr_addr = m_hdrs[hdr_idx];
		
		if (hdr_addr == 0) {
			throw new IllegalArgumentException("getFirstFixedForBlob called with unallocated address");
		}
		
		// read in header block, then free each reference
		final byte[] hdr = new byte[(blcks+1) * 4 + 4]; // add space for checksum
		m_store.getData(hdr_addr, hdr);
		
		final DataInputStream instr = new DataInputStream(
				new ByteArrayInputStream(hdr, 0, hdr.length-4) );
		try {
			final int nallocs = instr.readInt();
			final int faddr = instr.readInt();
			
			return faddr;
			
		} catch (IOException ioe) {
			throw new RuntimeException("Unable to retrieve first fixed address", ioe);
		}
	}

	public int getBlockSize() {
		// Not relevant for Blobs
		return 0;
	}

	public int getDiskAddr() {
		return m_diskAddr;
	}

	/**
	 * returns physical address of blob header if any.
	 */
	public long getPhysicalAddress(final int offset) {
		return m_store.physicalAddress(m_hdrs[offset]);
	}

	/**
	 * Since the Blob Allocator simply manages access to FixedAllocation blocks it does not manage any
	 * allocations directly.
	 */
	public int getPhysicalSize(final int offset) {
		return 0;
	}

	/**
	 * The startAddr
	 */
	public long getStartAddr() {
		// not relevant for blob
		return RWStore.convertAddr(m_sortAddr);
	}

	public String getStats(final AtomicLong counter) {
		return "";
	}

	/**
	 * hasFree if there are any non-zero entries in the m_hdr array;
	 */
	public boolean hasFree() {
		return m_freeSpots > 0;
	}

	public void preserveSessionData() {
		// all data held by fixed allocators
	}

	/**
	 * FIXME: There is a symmetry problem with read/write where one takes a Stream and the other
	 * return a byte[].  This is problematical with using the checksums.
	 */
	public void read(final DataInputStream str) {
		m_freeSpots = 0;
		try {
			for (int i = 0; i < 254; i++) {
				m_hdrs[i] = str.readInt();
				if (m_hdrs[i] == 0) m_freeSpots++;
			}
			final int chk = str.readInt();
			// checksum int chk = ChecksumUtility.getCHK().checksum(buf, str.size());

		} catch (IOException e) {
			log.error(e,e);
			throw new IllegalStateException(e);
		}
	}

	public void setDiskAddr(final int addr) {
		m_diskAddr = addr;
	}

	public void setFreeList(final ArrayList list) {
		m_freeList = list;

		if (hasFree()) {
			m_freeList.add(this);
		}
	}

	/**
	 * setIndex is called in two places, firstly to set the original index and secondly on restore
	 * from storage to re-establish the order.
	 * 
	 * When called initially, the m_startAddr will be zero and so must be set by retrieving the
	 * m_startAddr of the previous block (if any).  Now, since a Blob must use fixed allocations we
	 * are guaranteed that a BlobAllocator will not be the first allocator.  To derive a startAddr that
	 * can safely be used to sort a BlobAllocator against the previous (and subsequent) allocators we
	 * access the previous allocators address.
	 */
	public void setIndex(final int index) {
		m_index = index;
	}

	// @todo why is this a NOP?  Javadoc.
	public boolean verify(final int addr) {
		// TODO Auto-generated method stub
		return false;
	}

	public byte[] write() {
		try {
			final byte[] buf = new byte[1024]; // @todo why this const?
			final DataOutputStream str = new DataOutputStream(new FixedOutputStream(buf));
	
			str.writeInt(m_sortAddr);

			for (int i = 0; i < 254; i++) { // @todo why this const?
				str.writeInt(m_hdrs[i]);
			}
	
			// add checksum
			final int chk = ChecksumUtility.getCHK().checksum(buf, str.size());
			str.writeInt(chk);
	
			return buf;
		} catch (IOException ioe) {
			throw new IllegalStateException(ioe);
		}
	}

	public int compareTo(final Object o) {
		final Allocator alloc = (Allocator) o;
		
		assert getStartAddr() != alloc.getStartAddr();
		
		return (getStartAddr() < alloc.getStartAddr()) ? -1 : 1;
	}

	public int register(final int addr) {
		assert m_freeSpots > 0;
		
		m_store.addToCommit(this);
		
		for (int i = 0; i < 254; i++) {
			if (m_hdrs[i] ==  0) {
				m_hdrs[i] = addr;
				
				if (--m_freeSpots == 0) {
					m_freeList.remove(this);
				}
				
				final int ret = -((m_index << RWStore.OFFSET_BITS) + i);
				if (((-ret) & RWStore.OFFSET_BITS_MASK) > m_hdrs.length)
					throw new IllegalStateException("Invalid blob offset: " + ((-ret) & RWStore.OFFSET_BITS_MASK));
				
				return ret;
			}
		}

		return 0;
	}

	public int getRawStartAddr() {
		return m_sortAddr;
	}

	public int getIndex() {
		return m_index;
	}

	public int getBlobHdrAddress(final int hdrIndex) {
		return m_hdrs[hdrIndex];
	}

	public void appendShortStats(final StringBuilder str, final AllocationStats[] stats) {
		str.append("Index: " + m_index + ", address: " + getStartAddr() + ", BLOB\n");
	}

	public boolean isAllocated(final int offset) {
		return m_hdrs[offset] != 0;
	}

	/**
	 * This is okay as a NOP. The true allocation is managed by the 
	 * FixedAllocators.
	 */
	public void detachContext(final IAllocationContext context) {
		// NOP
	}

	/**
	 * Since the real allocation is in the FixedAllocators, this should delegate
	 * to the first address, in which case 
	 */
	public boolean canImmediatelyFree(final int addr, final int size, final IAllocationContext context) {
		final int faddr = this.getFirstFixedForBlob(addr, size);
		
		return m_store.getBlockByAddress(faddr).canImmediatelyFree(faddr, 0, context);
	}

}
