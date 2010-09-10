package com.bigdata.rwstore;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.rwstore.RWStore.AllocationStats;
import com.bigdata.util.ChecksumUtility;

/**
 * BlobAllocator
 * 
 * Manages Blob allocations using a list of FixedAllocators.
 * 
 * The main advantage of this is for re-allocation, since the FixedAllocators can be
 * efficiently re-cycled where a fixed Blob creates issues of best fit and fragmentation.
 * 
 * Some simple patterns would cause un-reallocatable storage, consider a Blob that always
 * re-allocated to a larger size, or a pattern where several blobs got larger together, in these
 * scenarios, smaller allocations would never be re-used, whilst the mechanism of component
 * based allocation is easily re-used.
 * 
 * @author mgc
 *
 */
public class BlobAllocator implements Allocator {
	int[] m_hdrs = new int[254];
	RWStore m_store;
	private int m_diskAddr;
	private int m_index;
	private int m_sortAddr;
	
	public BlobAllocator(RWStore store, int sortAddr) {
		m_store = store;
		m_sortAddr = sortAddr;
		
		System.out.println("New BlobAllocator");
	}
	
	public void addAddresses(ArrayList addrs) {
		// not relevant for BlobAllocators
	}

	public boolean addressInRange(int addr) {
		// not relevant for BlobAllocators
		return false;
	}

	public int alloc(RWStore store, int size, IAllocationContext context) {
		assert size > m_store.m_maxFixedAlloc;
		
		return 0;
	}

	public boolean free(int addr, int sze) {
		if (sze < m_store.m_maxFixedAlloc)
			throw new IllegalArgumentException("Unexpected address size");
		int alloc = m_store.m_maxFixedAlloc-4;
		int blcks = (alloc - 1 + sze)/alloc;		
		
		int hdr_idx = (-addr) & RWStore.OFFSET_BITS_MASK;
		if (hdr_idx > m_hdrs.length)
			throw new IllegalArgumentException("free BlobAllocation problem, hdr offset: " + hdr_idx + ", avail:" + m_hdrs.length);
		
		int hdr_addr = m_hdrs[hdr_idx];
		
		if (hdr_addr == 0) {
			return false;
		}
		
		// read in header block, then free each reference
		byte[] hdr = new byte[(blcks+1) * 4 + 4]; // add space for checksum
		m_store.getData(hdr_addr, hdr);
		
		try {
			DataInputStream instr = new DataInputStream(
					new ByteArrayInputStream(hdr, 0, hdr.length-4) );
			int allocs = instr.readInt();
			for (int i = 0; i < allocs; i++) {
				int nxt = instr.readInt();
				m_store.free(nxt, m_store.m_maxFixedAlloc);
			}
			m_store.free(hdr_addr, hdr.length);
			m_hdrs[hdr_idx] = 0;
			if (m_freeSpots++ == 0) {
				m_freeList.add(this);
			}
			
		} catch (IOException ioe) {
			
		}
		
		return false;
	}
	
	public int getFirstFixedForBlob(int addr, int sze) {
		if (sze < m_store.m_maxFixedAlloc)
			throw new IllegalArgumentException("Unexpected address size");

		int alloc = m_store.m_maxFixedAlloc-4;
		int blcks = (alloc - 1 + sze)/alloc;		
		
		int hdr_idx = (-addr) & RWStore.OFFSET_BITS_MASK;
		if (hdr_idx > m_hdrs.length)
			throw new IllegalArgumentException("free BlobAllocation problem, hdr offset: " + hdr_idx + ", avail:" + m_hdrs.length);
		
		int hdr_addr = m_hdrs[hdr_idx];
		
		if (hdr_addr == 0) {
			throw new IllegalArgumentException("getFirstFixedForBlob called with unallocated address");
		}
		
		// read in header block, then free each reference
		byte[] hdr = new byte[(blcks+1) * 4 + 4]; // add space for checksum
		m_store.getData(hdr_addr, hdr);
		
		try {
			DataInputStream instr = new DataInputStream(
					new ByteArrayInputStream(hdr, 0, hdr.length-4) );
			int nallocs = instr.readInt();
			int faddr = instr.readInt();
			
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
	public long getPhysicalAddress(int offset) {
		return m_store.physicalAddress(m_hdrs[offset]);
	}

	/**
	 * Since the Blob Allocator simply manages access to FixedAllocation blocks it does not manage any
	 * allocations directly.
	 */
	public int getPhysicalSize(int offset) {
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

	int m_freeSpots = 254;
	
	/**
	 * FIXME: There is a symmetry problem with read/write where one takes a Stream and the other
	 * return a byte[].  This is problematical with using the checksums.
	 */
	public void read(DataInputStream str) {
		m_freeSpots = 0;
		try {
			for (int i = 0; i < 254; i++) {
				m_hdrs[i] = str.readInt();
				if (m_hdrs[i] == 0) m_freeSpots++;
			}
			int chk = str.readInt();
			// checksum int chk = ChecksumUtility.getCHK().checksum(buf, str.size());

		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
	}

	public void setDiskAddr(int addr) {
		m_diskAddr = addr;
	}

	private ArrayList m_freeList;
	private long m_startAddr;

	public void setFreeList(ArrayList list) {
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
	public void setIndex(int index) {
		m_index = index;
	}

	public boolean verify(int addr) {
		// TODO Auto-generated method stub
		return false;
	}

	public byte[] write() {
		try {
			byte[] buf = new byte[1024];
			DataOutputStream str = new DataOutputStream(new FixedOutputStream(buf));
	
			str.writeInt(m_sortAddr);

			for (int i = 0; i < 254; i++) {
				str.writeInt(m_hdrs[i]);
			}
	
			// add checksum
			int chk = ChecksumUtility.getCHK().checksum(buf, str.size());
			str.writeInt(chk);
	
			return buf;
		} catch (IOException ioe) {
			throw new IllegalStateException(ioe);
		}
	}

	public int compareTo(Object o) {
		Allocator alloc = (Allocator) o;
		
		assert getStartAddr() != alloc.getStartAddr();
		
		return (getStartAddr() < alloc.getStartAddr()) ? -1 : 1;
	}

	public int register(int addr) {
		assert m_freeSpots > 0;
		
		m_store.addToCommit(this);
		
		for (int i = 0; i < 254; i++) {
			if (m_hdrs[i] ==  0) {
				m_hdrs[i] = addr;
				
				if (--m_freeSpots == 0) {
					m_freeList.remove(this);
				}
				
				int ret = -((m_index << RWStore.OFFSET_BITS) + i);
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

	public int getBlobHdrAddress(int hdrIndex) {
		return m_hdrs[hdrIndex];
	}

	public void appendShortStats(StringBuilder str, AllocationStats[] stats) {
		str.append("Index: " + m_index + ", address: " + getStartAddr() + ", BLOB\n");
	}

	public boolean isAllocated(int offset) {
		return m_hdrs[offset] != 0;
	}

	/**
	 * This is okay as a NOP. The true allocation is managed by the 
	 * FixedAllocators.
	 */
	public void detachContext(IAllocationContext context) {
		// NOP
	}

	/**
	 * Since the real allocation is in the FixedAllocators, this should delegate
	 * to the first address, in which case 
	 */
	public boolean canImmediatelyFree(int addr, int size, IAllocationContext context) {
		int faddr = this.getFirstFixedForBlob(addr, size);
		
		return m_store.getBlockByAddress(faddr).canImmediatelyFree(faddr, 0, context);
	}

}
