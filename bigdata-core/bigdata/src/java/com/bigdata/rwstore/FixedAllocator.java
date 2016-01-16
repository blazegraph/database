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

package com.bigdata.rwstore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.io.ChecksumUtility;
import com.bigdata.journal.AbstractJournal.ISnapshotData;
import com.bigdata.journal.ICommitter;
import com.bigdata.rawstore.IAllocationContext;
import com.bigdata.rwstore.RWStore.AllocationStats;
import com.bigdata.rwstore.StorageStats.Bucket;
import com.bigdata.util.BytesUtil;

/**
 * FixedAllocator
 * 
 * Maintains List of AllocBlock(s)
 */
public class FixedAllocator implements Allocator {
    
    private static final Logger log = Logger.getLogger(FixedAllocator.class);
    // Profiling for BLZG-1667 indicated that checking logging level is more expensive than expected
    private static final boolean s_islogDebug = log.isDebugEnabled();
    private static final boolean s_islogTrace = log.isTraceEnabled();

    private final int cModAllocation = 1 << RWStore.ALLOCATION_SCALEUP;
    private final int cMinAllocation = cModAllocation * 1; // must be multiple of cModAllocation

	volatile int m_freeBits;
	volatile private int m_freeTransients;
	
	FixedAllocator m_prevCommit;
	FixedAllocator m_nextCommit;

    /**
     * Address of the {@link FixedAllocator} within the meta allocation space on
     * the disk of the last committed version of this {@link FixedAllocator}.
     * This is a bit that can be decoded by {@link RWStore#metaBit2Addr(int)}.
     * The value is initially ZERO (0) which is never a legal bit.
     */
	volatile private int m_diskAddr;
	volatile private int m_index;
	
	Bucket m_statsBucket = null;
	
	/**
	 * If an allocator is selected in a smallSlotHighWaste scenario, then the sparseness test
	 * for allocation must be relaxed or there is a risk that no allocation would be made
	 * from a "free" allocator.
	 */
	boolean m_smallSlotHighWaste = false;
	
	public void setIndex(final int index) {
		final AllocBlock fb = (AllocBlock) m_allocBlocks.get(0);
		
        if (s_islogDebug)
            log.debug("Restored index " + index + " with " + getStartAddr()
                    + "[" + fb.m_live[0] + "] from " + m_diskAddr);

		m_index = index;
	}

	public long getStartAddr() {
//		if (m_startAddr == 0) {
//			log.warn("zero m_startAddr, setting to " + m_allocBlocks.get(0).m_addr);
//			
//			m_startAddr = m_allocBlocks.get(0).m_addr;
//		}
		return RWStore.convertAddr(m_startAddr);
	}

    /*
     * Note: Object#equals() is fine with this compareTo() implementation. It is
     * only used to sort the allocators.
     */
	public int compareTo(final Object o) {
		final Allocator other = (Allocator) o;
		if (other.getStartAddr() == 0) {
			return -1;
		} else {
			final long val = getStartAddr() - other.getStartAddr();

			if (val == 0) {
				throw new Error("Two allocators at same address");
			}

			return val < 0 ? -1 : 1;
		}
	}
	
	public boolean equals(final Object o) {
		return this == o;
	}

    /**
     * Return the bit in metabits for the last persisted version of this
     * allocator. This bit can be translated into the actual byte offset on the
     * file by {@link RWStore#metaBit2Addr(int)}.
     */
	public int getDiskAddr() {
		return m_diskAddr;
	}

    /**
     * Set the bit in metabits for this allocator.
     * 
     * @param addr
     *            A bit obtained from {@link RWStore#metaAlloc()}.
     */
	public void setDiskAddr(final int addr) {
		if (m_index == -1) {
			throw new IllegalStateException("Attempt to set a storage addr for an invalid FixedAllcator");
		}

		m_diskAddr = addr;
	}

    /**
     * Return the byte offset on the file corresponding to a bit index into this
     * {@link FixedAllocator}.
     * <p>
     * The tweak of 3 to the offset is to ensure 1, that no address is zero and
     * 2 to enable the values 1 & 2 to be special cased (this aspect is now
     * historical).
     * 
     * @param offset
     *            The bit index into the {@link FixedAllocator}.
     * 
     * @return The byte offset on the backing file.
     */
	public long getPhysicalAddress(int offset, final boolean nocheck) {
	  	offset -= 3;

	  	// The AllocBlock that manages that bit.
        final AllocBlock block = (AllocBlock) m_allocBlocks.get(offset
                / allocBlockRange);
		
        // The bit offset into the AllocBlock.
		final int bit = offset % allocBlockRange;
		
//		if (RWStore.tstBit(block.m_live, bit) 
//				|| (m_sessionActive && RWStore.tstBit(block.m_transients, bit))) { 

        /*
         * Compute the offset into the region managed by that AllocBlock and
         * then add it to the byte offset of the AllocBlock on the backing file.
         * This gives us the total offset on the backing file associated with
         * that bit.
         */
		final long paddr = RWStore.convertAddr(block.m_addr) + ((long) m_size * bit);
		
		/*
		 * Just check transients since there are case (eg CommitRecordIndex)
		 * where committed data is accessed even if has been marked as ready to
		 * be recycled after the next commit
		 */
		if (nocheck || RWStore.tstBit(block.m_transients, bit)) {			
		
		    return paddr;
		    
		} else {
			
		    if (RWStore.tstBit(block.m_commit, bit)) {
                throw new IllegalStateException(
                        "Address committed but not set in transients");
            }
			
		    m_store.showWriteCacheDebug(paddr);			
			
		    log.warn("Physical address " + paddr + " not accessible for Allocator of size " + m_size);
			
			return 0L;
		}
	}

	public int getPhysicalSize(final int offset) {
		return m_size;
	}

	public int getBlockSize() {
		return m_size;
	}

	/**
	 * The free list for the allocation slot size serviced by this allocator.
	 * This is a reference back into the corresponding free list as managed by
	 * the RWStore.
	 * 
	 * @see #setFreeList(ArrayList)
	 */
	private ArrayList m_freeList;

	public void setFreeList(final ArrayList list) {
		setFreeList(list, false);
		
	}
	/**
	 * The force parameter is set to true when the allocator is being moved from one
	 * free list to another.
	 */
	public void setFreeList(final ArrayList list, boolean force) {
		if (m_freeList != list) {
			m_freeList = list;
			m_freeWaiting = true;
		}

		if (m_pendingContextCommit || !hasFree()) {
			if (force) {
				throw new IllegalStateException("The allocator cannot be added to the free list, pendingContextCommit: " + m_pendingContextCommit + ", hasFree: " + hasFree());
			}
			
			return;
		}
		
		if (force || meetsSmallSlotThreshold()) {
			addToFreeList();
		}
		
	}
	
	/**
	 * To support postHACommit an allocator can be removed from the current freelist
	 */
	void removeFromFreeList() {
		if (m_freeList != null) {
			// log.warn("Removing allocator " + m_index + " from free list");
			m_freeList.remove(this);
			m_freeWaiting = true;
		}
		
	}

	volatile private IAllocationContext m_context;

	/**
	 * @return whether the allocator is unassigned to an AllocationContext
	 */
	boolean isUnlocked() {
		return m_context == null;
	}
	
	/**
	 * Indicates whether session protection has been used to protect
	 * store from re-allocating allocations reachable from read-only
	 * requests and concurrent transactions.
	 */
	private boolean m_sessionActive;

	boolean m_pendingContextCommit = false; // accessible from RWStore
	
	public void setAllocationContext(final IAllocationContext context) {
		if (m_pendingContextCommit) {
			throw new IllegalStateException("Already pending commit");
		}
		
        if (s_islogDebug)
            checkBits();

		if (context == null && m_context != null) {
			// restore commit bits in AllocBlocks
			for (AllocBlock allocBlock : m_allocBlocks) {
				allocBlock.deshadow();
			}
			
			// return to dirty list
			m_store.addToCommit(this);
			m_pendingContextCommit  = true;
			
		} else if (context != null && m_context == null) {
			// restore commit bits in AllocBlocks
			for (AllocBlock allocBlock : m_allocBlocks) {
				allocBlock.shadow();
			}
			
			// remove from dirty list if present!
			// NO! m_store.removeFromCommit(this);
		}
		m_context = context;
		
        if (s_islogDebug)
            checkBits();

	}

	/**
	 * Unwinds the allocations made within the context and clears the write
	 * cache of any associated data
	 * @param writeCacheService 
	 */
	public void abortAllocationContext(final IAllocationContext context, RWWriteCacheService writeCacheService) {
		if (m_pendingContextCommit) {
			throw new IllegalStateException("Already pending commit");
		}

		if (m_context != null) {
			// restore commit bits in AllocBlocks
			for (AllocBlock allocBlock : m_allocBlocks) {
				allocBlock.abortshadow(writeCacheService);
			}
			
			// Reset freebits
			m_freeBits = calcFreeBits();
			
			m_context = context;
		} else {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * write called on commit, so this is the point when "transient frees" - the
	 * freeing of previously committed memory can be made available since we
	 * are creating a new commit point - the condition being that m_freeBits
	 * was zero and m_freeTransients not.
	 */
	public byte[] write() {
		try {
			
			final AllocBlock fb = m_allocBlocks.get(0);
			if (s_islogTrace)
				log.trace("writing allocator " + m_index + " for " + getStartAddr() + " with " + fb.m_live[0]);
			final byte[] buf = new byte[1024];
			final DataOutputStream str = new DataOutputStream(new FixedOutputStream(buf));
			final boolean protectTransients = m_sessionActive || m_store.isSessionProtected();
			try {
                str.writeInt(m_size);
                
                assert m_sessionActive || m_freeTransients == transientbits();
    			
                final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
                while (iter.hasNext()) {
                    final AllocBlock block = iter.next();

                    str.writeInt(block.m_addr);
                    for (int i = 0; i < m_bitSize; i++) {
                    	    str.writeInt(block.m_live[i]);
                    }

                    if (!protectTransients) {
                    	/**
                    	 * This assert will trip if any address was freed under
                    	 * session protection and therefore remained accessible
                    	 * until released.
                    	 * The value returned by releaseSession should be zero
                    	 * since all "frees" should already have removed any 
                    	 * writes to the writeCacheService
                    	 */
                    	assert m_sessionFrees.intValue() == 0;
                    	// assert block.releaseSession(m_store.m_writeCache) == 0;
                    	
                    	// clear out writes - FIXME is releaseSession okay
                    	block.releaseCommitWrites(m_store.getWriteCacheService());
                    	
                    	// Moved to postCommit()
                        // block.m_transients = block.m_live.clone();
                    }

                }
                // add checksum
                final int chk = ChecksumUtility.getCHK().checksum(buf,
                        str.size());
                str.writeInt(chk);
			} finally {
			    str.close();
			}

            if (s_islogDebug)
                checkBits();
			
			return buf;
		} catch (IOException e) {
			throw new StorageTerminalError("Error on write", e);
		}
	}

	private int calcFreeBits() {
		int freeBits = 0;
		
		final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
//		final int blockSize = m_bitSize * 32 * m_size;
		while (iter.hasNext()) {
			final AllocBlock block = iter.next();
			for (int i = 0; i < m_bitSize; i++) {
				freeBits += 32 - Integer.bitCount(block.m_transients[i]);
			}
		}
		
		return freeBits;
	}

	private int calcLiveFreeBits() {
		int freeBits = 0;
		
		final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
//		final int blockSize = m_bitSize * 32 * m_size;
		while (iter.hasNext()) {
			final AllocBlock block = iter.next();
			for (int i = 0; i < m_bitSize; i++) {
				freeBits += 32 - Integer.bitCount(block.m_live[i]);
			}
		}
		
		return freeBits;
	}
	
	private boolean checkBits() {
		final int calcFree = calcFreeBits();
		final int calcLiveFree = calcLiveFreeBits();
		
		final boolean ret =  m_freeBits == calcFree
			&& (m_freeBits + m_freeTransients) == calcLiveFree;
				
		if (!ret)
			throw new AssertionError("m_free: " + m_freeBits + ", calcFree: " + calcFree);
		
		return ret;		
	}

	// read does not read in m_size since this is read to determine the class of
	// allocator
	public void read(final DataInputStream str) {
		try {
			m_freeBits = 0;

			final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
			final int blockSize = m_bitSize * 32 * m_size;
			while (iter.hasNext()) {
				final AllocBlock block = iter.next();

				block.m_addr = str.readInt();
				for (int i = 0; i < m_bitSize; i++) {
					block.m_live[i] = str.readInt();

					/**
					 * Need to calc how many free blocks are available, minor
					 * optimization by checking against either empty or full to
					 * avoid scanning every bit unnecessarily
					 **/
					if (block.m_live[i] == 0) { // empty
						m_freeBits += 32;
					} else if (block.m_live[i] != 0xFFFFFFFF) { // not full
						final int anInt = block.m_live[i];
//						for (int bit = 0; bit < 32; bit++) {
//							if ((anInt & (1 << bit)) == 0) {
//								m_freeBits++;
//							}
//						}
						
						m_freeBits += 32 - Integer.bitCount(anInt);
					} // else full so no freebits
				}

				block.m_transients = (int[]) block.m_live.clone();
				block.m_commit = (int[]) block.m_live.clone();

				if (m_startAddr == 0) {
					m_startAddr = block.m_addr;
				}
				// int endAddr = block.m_addr + blockSize;
				if (block.m_addr > 0) {
					m_endAddr = block.m_addr + blockSize;
				}
			}
		} catch (IOException e) {
			throw new StorageTerminalError("Error on read", e);
		}

	}

    /** The size of the allocation slots in bytes. */
    final int m_size;

	private int m_startAddr = 0;
	private int m_endAddr = 0;
	
	/**
	 * For "small slot" allocators the allocation search is
	 * always from bit areas with less than a maximum density to
	 * ensure that writes have better locality.
	 */
	int m_allocIndex = -1;

    /**
     * The #of int32 values in a single {@link AllocBlock} region. The
     * {@link FixedAllocator} can manage many {@link AllocBlock}s.
     */
    private final int m_bitSize;

    /**
     * The #of bits in an {@link AllocBlock} (the #of slots managed by that
     * {@link AllocBlock}). Each slot managed by the {@link AllocBlock} is
     * {@link #m_size} bytes.
     */
    private final int allocBlockRange;

	private final ArrayList<AllocBlock> m_allocBlocks;

	final RWStore m_store;

	/**
	 * Calculating the number of ints (m_bitSize) cannot rely on a power of 2.  Previously this
	 * assumption was sufficient to guarantee a rounding on to an 64k boundary.  However, now
	 * nints * 32 * 64 = 64K, so need multiple of 32 ints.
	 * <p>
	 * So, whatever multiple of 64, if we allocate a multiple of 32 ints we are guaranteed to be 
	 * on an 64K boundary.
	 * <p>
	 * This does mean that for the largest blocks of ~256K, we are allocating 256Mb of space
	 * 
	 * @param size The size of the allocation slots in bytes.
	 * @param preserveSessionData
	 * @param cache
	 */
	FixedAllocator(final RWStore store, final int size) {//, final RWWriteCacheService cache) {

	    // Note: ZERO (0) is never a valid metabits bit.
	    m_diskAddr = 0;
		m_store = store;

		m_size = size;

		// By default, disk-based allocators should optimize for density
		m_bitSize = calcBitSize(true /* optDensity */, size, cMinAllocation, cModAllocation);

		// The #of bits in an AllocBlock.
		allocBlockRange = 32 * m_bitSize;
		 
		// number of blocks in this allocator, bitSize plus 1 for start address
		// The 1K allocator is 256 ints, one is used to record the slot size and
		// another for the checksum; leaving 254 to be used to store the 
		// AllocBlocks.
		final int numBlocks = 254 / (m_bitSize + 1);

		/*
		 * Create AllocBlocks for this FixedAllocator, but do not allocate
		 * either the AllocBlocks or their managed allocation slots on the
		 * persistent heap yet.
		 */
		m_allocBlocks = new ArrayList<AllocBlock>(numBlocks);
		for (int i = 0; i < numBlocks; i++) {
			m_allocBlocks.add(new AllocBlock(0, m_bitSize, this));//, cache));
		}

		m_freeTransients = 0;
		m_freeBits = 32 * m_bitSize * numBlocks;
	}
	
	/**
	 * find the allocationIndex of first "sparsely committed" AllocBlock.
	 * 
	 * Checks the committed bits of all the AllocBlocks until one is found with
	 * > 50% free (or less than 50% allocated) of the committed bits.
	 * @param store 
	 * @param i 
	 */
	void resetAllocIndex() {
		resetAllocIndex(0);
	}
	
	void resetAllocIndex(final int start) {
		m_allocIndex = start;
		
		if (m_size <= m_store.cSmallSlot) {
	    	
			for (int a = m_allocIndex/m_bitSize; a < m_allocBlocks.size(); a++) {
				final AllocBlock ab = m_allocBlocks.get(a);
				
				checkBlock(ab);
				
				for (int i = (m_allocIndex%m_bitSize); i < m_bitSize; i++) {
					// first check if transients are already full
					if (ab.m_transients[i] != 0xFFFFFFFF) {
						/*
						 * If small slots are in a high waste scenario, then do not check for extra
						 * locality in uncommitted state
						 */
						if (m_smallSlotHighWaste || Integer.bitCount(ab.m_commit[i]) < 16) { 
							final AllocBlock abr = m_allocBlocks.get(m_allocIndex/m_bitSize);
							assert abr == ab;
							
							return;
						}
					}
					m_allocIndex++;
				}
			}
		
			// must remove from free list if we cannot set the alloc Index for a small slot
			if (start == 0) {
				removeFromFreeList();
			} else {
				resetAllocIndex(0);
			}
		}
	}
	
	/**
	 * This determines the size of the reservation required in terms of
	 * the number of ints each holding bits for 32 slots.
	 * 
	 * The minimum return value will be 1, for a single int holiding 32 bits.
	 * 
	 * The maximum value will be the number of ints required to fill the minimum
	 * reservation.
	 * 
	 * The minimum reservation will be some multiple of the
	 * address multiplier that allows alloction blocks to address large addresses
	 * with an INT32.  For example, by setting a minimum reservation at 128K, the
	 * allocation blocks INT32 start address may be multiplied by 128K to provide
	 * a physical address.
	 * 
	 * The minReserve must be a power of 2, eg 1K, 2k or 4K.. etc
	 * 
	 * A standard minReserve of 16K is plenty big enough, enabling 32TB of
	 * addressable store.  The logical maximum used store is calculated as the
	 * maximum fixed allocation size * MAX_INT.  So a store with a maximum
	 * fixed slot size of 4K could only allocated 8TB.
	 * 
	 * Since the allocation size must be MOD 0 the minReserve, the lower the 
	 * minReserve the smaller the allocation may be required for larger
	 * slot sizes.
	 * 
	 * Another consideration is file locality.  In this case the emphasis is
	 * on larger contiguous areas to improve the likely locality of allocations
	 * made by a FixedAllocator.  Here the addressability implied by the reserve
	 * is not an issue, and larger reserves are chosen to improve locality.  The
	 * downside is a potential for more wasted space, but this
	 * reduces as the store size grows and in large stores (> 10GB) becomes
	 * insignificant.
	 * 
	 * Therefore, if a FixedAllocator is to be used in a large store and
	 * locality needs to be optimised for SATA disk access then the minReserve
	 * should be high = say 128K, while if the allocator is tuned to ByteBuffer
	 * allocation, a minallocation of 8 to 16K is more suitable.
	 * 
	 * A final consideration is allocator reference efficiency in the sense
	 * to maximise the amount of allocations that can be made.  By this I mean
	 * just how close we can get to MAX_INT allocations.  For example, if we
	 * allow for upto 8192 allocations from a single allocator, but in
	 * practice average closer to 4096 then the maximum number of allocations
	 * comes down from MAX_INT to MAX_INT/2.  This is also a consideration when
	 * considering max fixed allocator size, since if we require a large number
	 * of Blobs this reduces the amount of "virtual" allocations by at least
	 * a factro of three for each blob (at least 2 fixed allocations for
	 * content and 1 more for the header).  A variation on the current Blob
	 * implementation could include the header in the first allocation, thus
	 * reducing the minimum Blob allocations from 3 to 2, but the point still
	 * holds that too small a max fixed allocation could dramatically reduce the
	 * number of allocations that could be made.
	 * 
	 * @param alloc the slot size to be managed
	 * @param minReserve the minimum reservation in bytes
	 * @return the size of the int array
	 */
	public static int calcBitSize(final boolean optDensity, final int alloc, final int minReserve, final int modAllocation) {
		final int intAllocation = 32 * alloc; // min 32 bits
		
		// we need to find smallest number of ints * the intAllocation
		//	such that totalAllocation % minReserve is 0
		// example 6K intAllocation would need 8 ints for 48K for 16K min
		// likewise a 24K intAllocation would require 2 ints
		 // if optimising for density set min ints to 8
		int nints = optDensity ? 8 : 1;
		while ((nints * intAllocation) < minReserve) nints++;
		
		while ((nints * intAllocation) % modAllocation != 0) nints++;
		
		return nints;
	}

	public String getStats(final AtomicLong counter) {

        final StringBuilder sb = new StringBuilder(getSummaryStats());

		final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
		while (iter.hasNext()) {
			final AllocBlock block = iter.next();
			if (block.m_addr == 0) {
				break;
			}
            sb.append(block.getStats(null) + "\r\n");
            if (counter != null)
                counter.addAndGet(block.getAllocBits() * (long) m_size);
		}

		return sb.toString();
	}

	public String getSummaryStats() {

        return"Block size : " + m_size
                + " start : " + getStartAddr() + " free : " + m_freeBits
                + "\r\n";
	}

	public boolean verify(int addr) {
		if (addr >= m_startAddr && addr < m_endAddr) {

			final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
			while (iter.hasNext()) {
				final AllocBlock block = iter.next();
				if (block.verify(addr, m_size)) {
					return true;
				}
			}
		}

		return false;
	}

	public boolean addressInRange(int addr) {
		if (addr >= m_startAddr && addr < m_endAddr) {

			final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
			while (iter.hasNext()) {
				final AllocBlock block = iter.next();
				if (block.addressInRange(addr, m_size)) {
					return true;
				}
			}
		}

		return false;
	}

	private boolean m_freeWaiting = true;

	// track number of frees to be cleared on session releases
	private AtomicInteger m_sessionFrees = new AtomicInteger(0);
	
	public boolean free(final int addr, final int size) {
		return free(addr, size, false);
	}
	
	/**
	 * Need to check if address to be freed was 'live' for any shadowed allocator to
	 * determine if we need to adjust the 'savedLive' data.  This is critical since
	 * otherwise we will not be able to reset any unisolated alloc/frees.
	 */
	public boolean free(final int addr, final int size, final boolean overideSession) {
		if (addr < 0) {
			final int offset = ((-addr) & RWStore.OFFSET_BITS_MASK) - 3; // bit adjust

			final int nbits = 32 * m_bitSize;

			final int block = offset/nbits;
			
			/**
			 * When a session is released any m_sessionActive FixedAllocators
			 * should be atomically released.
			 * However, if any state allowed a call to free once the store
			 * is not session protected, this must NOT overwrite m_sessionActive
			 * if it is already set since a commit would reset the transient bits
			 * without first clearing addresses them from the writeCacheService
			 */
			final boolean tmp = m_sessionActive;
			m_sessionActive = tmp || m_store.isSessionProtected();
			if (tmp && !m_sessionActive) throw new AssertionError();
			
			try {
	            if (s_islogDebug)
	                checkBits();
	            
				if (((AllocBlock) m_allocBlocks.get(block))
						.freeBit(offset % nbits, m_sessionActive && !overideSession)) { // bit adjust
					
					m_freeBits++;

					checkFreeList();

				} else {
					m_freeTransients++;
					if (m_sessionActive) {
						assert checkSessionFrees();
					}						
				}				
				
				if (m_statsBucket != null) {
					m_statsBucket.delete(size);
				}
			} catch (IllegalArgumentException iae) {
				// catch and rethrow with more information
				throw new IllegalArgumentException("IAE with address: " + addr + ", size: " + size + ", context: " + (m_context == null ? -1 : m_context.hashCode()), iae);
			}

            if (s_islogDebug)
                checkBits();

    		return true;
		} else if (addr >= m_startAddr && addr < m_endAddr) {

			final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
			while (iter.hasNext()) {
				final AllocBlock block = iter.next();
				if (block.free(addr, m_size)) {
					m_freeTransients++;

		            if (s_islogDebug)
		                checkBits();

					return true;
				}
			}
		}
		
        if (s_islogDebug)
            checkBits();

		return false;
	}

	private boolean checkSessionFrees() {
		final int sessionFrees = m_sessionFrees.incrementAndGet();
		int sessionBits = 0;
		for (AllocBlock ab : m_allocBlocks) {
			sessionBits += ab.sessionBits();
		}
		return sessionFrees <= sessionBits;	
	}
	
	private void checkFreeList() {
		if (m_freeWaiting && !m_pendingContextCommit) {
			if (meetsSmallSlotThreshold()) {
				
				addToFreeList();
				
				resetAllocIndex(0);				
			}
		}
	}
	
	void addToFreeList() {
		assert m_freeWaiting;
		
		m_freeWaiting = false;
		m_freeList.add(this);
		m_allocIndex = -1;
		
		if (s_islogDebug)
			log.debug("Returning Allocator to FreeList - " + m_size);
	}
	
	private boolean meetsSmallSlotThreshold() {
		// check threshold for all slots
		if (m_freeBits < m_store.cDefaultFreeBitsThreshold) {
			return false;
		}
		
		// then check for small slots
		if (m_size <= m_store.cSmallSlot) { // it's a small slot
			final boolean ret =  m_freeBits > m_store.cSmallSlotThreshold;
			return ret;
		} else {
			return true;
		}
	}

	/**
	 * The introduction of IAllocationContexts has added some complexity to
	 * the older concept of a free list.  With AllocationContexts it is
	 * possibly for allocator to have free space available but this being
	 * restricted to a specific AllocationContext.
	 * <p>
	 * In addition to the standard free allocation search we want to add a
	 * "density" restriction for small slots to encourage the aggregation
	 * of writes (by increasing the likelihood of sibling slot allocation).
	 * <p>
	 * There is some "Do What I mean" complexity here, with difficulty in 
	 * determining a good rule to identify an initial allocation point.  There
	 * is a danger of significantly reducing the allocation efficiency of
	 * short transactions if we too naively check committed bit density.  We
	 * should only do this when identifying the initial allocation, and when
	 * the allocIndex is incremented.
	 */
	public int alloc(final RWStore store, final int size,
			final IAllocationContext context) {
		try {
			if (size <= 0)
				throw new IllegalArgumentException(
						"Allocate requires positive size, got: " + size);

			if (size > m_size)
				throw new IllegalArgumentException(
						"FixedAllocator with slots of " + m_size
								+ " bytes requested allocation for " + size
								+ " bytes");

			if (m_freeBits == 0) {
				throw new IllegalStateException("Request to allocate from " + m_size + "byte slot FixedAllocator with zero bits free - should not be on the Free List");
			}
			
			int addr = -1;
			
			// Special allocation for small slots
			if (m_size <= m_store.cSmallSlot) {
				return allocFromIndex(size);
			}

			final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
			int count = -1;
			while (addr == -1 && iter.hasNext()) {
				count++;

				final AllocBlock block = iter.next();
				checkBlock(block);
				
				addr = block.alloc(m_size);
			}

			if (addr != -1) {

				addr += 3; // Tweak to ensure non-zero address for offset 0

				if (--m_freeBits == 0) {
					if (s_islogTrace)
						log.trace("Remove from free list");
					removeFromFreeList();

					// Should have been first on list, now check for first
					if (m_freeList.size() > 0) {
						if (s_islogDebug) {
							final FixedAllocator nxt = (FixedAllocator) m_freeList
									.get(0);
							log.debug("Freelist head: " + nxt.getSummaryStats());
						}
					}
				}

				addr += (count * 32 * m_bitSize);

				final int value = -((m_index << RWStore.OFFSET_BITS) + addr);

				if (m_statsBucket != null) {
					m_statsBucket.allocate(size);
				}

				return value;
			} else {
				StringBuilder sb = new StringBuilder();
				sb.append("FixedAllocator returning null address, with freeBits: "
						+ m_freeBits + "\n");

				for (AllocBlock ab : m_allocBlocks) {
					sb.append(ab.show() + "\n");
				}

				log.error(sb);

				return 0;
			}
		} finally {
			if (s_islogDebug)
				checkBits();
		}
	}
	
	boolean checkBlock0() {
		return checkBlock(m_allocBlocks.get(0));
	}
	
	boolean checkBlock(final AllocBlock block) {
		if (block.m_addr == 0) {
			int blockSize = 32 * m_bitSize;
			if (m_statsBucket != null) {
				m_statsBucket.addSlots(blockSize);
			}
			blockSize *= m_size;
			blockSize >>= RWStore.ALLOCATION_SCALEUP;

			block.m_addr = grabAllocation(m_store, blockSize);
			if (s_islogDebug)
				log.debug("Allocation block at " + block.m_addr
						+ " of " + (blockSize << 16) + " bytes");

			if (m_startAddr == 0) {
				m_startAddr = block.m_addr;
			}
			m_endAddr = block.m_addr - blockSize;
			
			return true; // commit required
		} else {
			return false;
		}

	}
	
	int allocFromIndex(final int size) {
		
		if (m_allocIndex == -1) {
			resetAllocIndex();
			
			if (m_allocIndex == -1) {
				throw new AssertionError("Unable to set AllocIndex with m_freeBits: " + m_freeBits);
			}
		}
		
        if (s_islogDebug)
            checkBits();


        if (s_islogDebug) { // calcFreeBits is relatively expensive, so only enable in DEBUG
			if (m_freeBits != calcFreeBits()) {
				final int calc = calcFreeBits();
				throw new AssertionError("m_freeBits != calcFreeBits() : " + m_freeBits + "!=" + calc);
			}
        }
        assert m_freeBits == calcFreeBits();

		// there MUST be bits free in the m_allocIndex block
		final AllocBlock ab = m_allocBlocks.get(m_allocIndex/m_bitSize);
		
		if (ab.m_addr == 0) {
			throw new AssertionError("No allocation for AllocBlock with m_allocIndex: " + m_allocIndex);
		}
		
		final int abblock = m_allocIndex % m_bitSize;
		
		assert ab.m_transients[abblock] != 0xFFFFFFFF; // not all set
		
		final int bit = RWStore.fndBit(ab.m_transients[abblock]);
		
		assert bit >= 0;
		
		m_freeBits--;
		
		final int abit = (abblock*32) + bit;
		RWStore.setBit(ab.m_live, abit);
		RWStore.setBit(ab.m_transients, abit);
		
		// Note +3 for address teak for special low order bits
		final int addr = -((m_index << RWStore.OFFSET_BITS) + (m_allocIndex*32) + (bit + 3));
		
		// Now check current index
		if (ab.m_transients[abblock] == 0xFFFFFFFF) {
			// find next allocIndex
			resetAllocIndex(m_allocIndex+1);
		}
		
      if (s_islogDebug) { // calcFreeBits is relatively expensive, so only enable in DEBUG
			final int calc = calcFreeBits();
			if (m_freeBits != calc) {
				throw new AssertionError("m_freeBits != calcFreeBits() : " + m_freeBits + "!=" + calc);
			}
      }
		// assert m_freeBits == calcFreeBits();

		if (m_statsBucket != null) {
			m_statsBucket.allocate(size);
		}

		return addr;
	}

	protected int grabAllocation(RWStore store, int blockSize) {
		
		final int ret =  store.allocBlock(blockSize);
		
		return ret;
	}

	public boolean hasFree() {
		return m_freeBits > 0;
	}

	public void addAddresses(final ArrayList addrs) {
		
		final Iterator blocks = m_allocBlocks.iterator();

		// FIXME int baseAddr = -((m_index << 16) + 4); // bit adjust
		int baseAddr = -(m_index << 16); // bit adjust??

		while (blocks.hasNext()) {
		    final AllocBlock block = (AllocBlock) blocks.next();

			block.addAddresses(addrs, baseAddr);

			baseAddr -= 32 * m_bitSize;
		}
	}

	/**
	 * returns the raw start address
	 */
	public int getRawStartAddr() {
		return m_startAddr;
	}

	public int getIndex() {
		return m_index;
	}

    public void appendShortStats(final StringBuilder str,
            final AllocationStats[] stats) {

		int si = -1;

		if (stats == null) {
			str.append("Index: " + m_index + ", " + m_size);
		} else {		
			for (int i = 0; i < stats.length; i++) {
				if (m_size == stats[i].m_blockSize) {
					si = i;
					break;
				}
			}
		}
		
		final Iterator<AllocBlock> blocks = m_allocBlocks.iterator();
		while (blocks.hasNext()) {
			final AllocBlock block = blocks.next();
			if (block.m_addr != 0) {
				str.append(block.getStats(si == -1 ? null : stats[si]));
			} else {
				break;
			}
		}
		str.append("\n");
	}
	
	public int getAllocatedBlocks() {
		int allocated = 0;
		final Iterator<AllocBlock> blocks = m_allocBlocks.iterator();
		while (blocks.hasNext()) {
			if (blocks.next().m_addr != 0) {
				allocated++;
			} else {
				break;
			}
		}

		return allocated;
	}
	
	/**
	 * @return  the amount of heap storage assigned to this allocator over
	 * all reserved allocation blocks.
	 */
	public long getFileStorage() {
		
	    final long blockSize = 32L * m_bitSize * m_size;
		
		long allocated = getAllocatedBlocks();

		allocated *= blockSize;

		return allocated;
	}
	
	/**
	 * Computes the amount of storage allocated using the freeBits count.
	 * 
	 * @return the amount of storage to alloted slots in the allocation blocks
	 */
	public long getAllocatedSlots() {
		final int allocBlocks = getAllocatedBlocks();
		int xtraFree = m_allocBlocks.size() - allocBlocks;
		xtraFree *= 32 * m_bitSize;
		
		final int freeBits = m_freeBits - xtraFree;
		
		final long alloted = (allocBlocks * 32 * m_bitSize) - freeBits;
		
		return alloted * m_size;		
	}

	public boolean isAllocated(int offset) {
	  	offset -= 3;

	  	final int allocBlockRange = 32 * m_bitSize;

		final AllocBlock block = (AllocBlock) m_allocBlocks.get(offset / allocBlockRange);
		
		final int bit = offset % allocBlockRange;
		
		return RWStore.tstBit(block.m_live, bit);
	}

	public boolean isCommitted(int offset) {
	  	offset -= 3;

	  	final int allocBlockRange = 32 * m_bitSize;

		final AllocBlock block = (AllocBlock) m_allocBlocks.get(offset / allocBlockRange);
		
		final int bit = offset % allocBlockRange;
		
		return RWStore.tstBit(block.m_commit, bit);
	}

	protected final AllocBlock getBlockFromLocalOffset(int offset) {
	  	offset -= 3;

	  	final int allocBlockRange = 32 * m_bitSize;

		return (AllocBlock) m_allocBlocks.get(offset / allocBlockRange);
	}

	/**
	 * If the context is this allocators context AND it is not in the commit bits
	 * then we can immediately free.
	 */
    public boolean canImmediatelyFree(final int addr, final int size,
            final IAllocationContext context) {
		final int offset = ((-addr) & RWStore.OFFSET_BITS_MASK); // bit adjust
		final boolean committed = isCommitted(offset);

		if (!m_pendingContextCommit && ((context == m_context) || (m_context == null && !context.isIsolated()))) {
			
			return !committed;
		} else if (m_context != null) {
			// This must *not* be an address transiently allocated by the associated Allocator
			if (!committed)
				throw new IllegalStateException("Attempt to free address with invalid context");
			
			return false;
		} else {
			return false;
		}
	}
    
	public void setBucketStats(Bucket b) {
		m_statsBucket = b;
	}
	
	/**
	 * The semantics of reset are to ditch all unisolated modifications
	 * since the last commit point.  Note that this includes unisolated frees
	 * as well as allocations.
	 * 
	 * @param cache
	 * @param nextAllocation 
	 */
	boolean reset(RWWriteCacheService cache, final int nextAllocation) {
		boolean isolatedWrites = false;
		for (AllocBlock ab : m_allocBlocks) {
			if (ab.m_addr == 0)
				break;
			
			ab.reset(cache);
			
			isolatedWrites = isolatedWrites || ab.m_saveCommit != null;
			
			if (ab.m_addr <= nextAllocation && ab.m_saveCommit == null) { // only free if no isolated writes
				// this should mean that all allocations were made since last commit
				// in which case...
				assert ab.freeBits() == ab.totalBits();
				
				ab.m_addr = 0;
			}
		}
		
		m_freeTransients = transientbits();
		m_freeBits = calcFreeBits();
		
		// Ensure allocIndex is reset
		m_allocIndex = -1;
		
		assert calcSessionFrees();
		
		if (s_islogDebug)
			checkBits();
		
		return isolatedWrites;
	}
	
	private boolean calcSessionFrees() {
		int sessionBits = 0;
		for (AllocBlock ab : m_allocBlocks) {
			sessionBits += ab.sessionBits();
		}
		m_sessionFrees.set(sessionBits);
		
		return true;
	}

	void releaseSession(RWWriteCacheService cache) {
		if (m_context != null) {
			throw new IllegalStateException("Calling releaseSession on shadowed allocator");
		}
		
		if (this.m_sessionActive) {
			final int start = m_sessionFrees.intValue();
			// try {
				if (s_islogTrace)
					log.trace("Allocator: #" + m_index + " releasing session protection");
				

				int releasedAllocations = 0;
				for (AllocBlock ab : m_allocBlocks) {
					releasedAllocations += ab.releaseSession(cache);
				}
				
				assert !m_store.isSessionProtected() : "releaseSession called with isSessionProtected: true";
				
				m_sessionActive = false; // should only need indicate that it contains no cached writes
	
				
				m_freeBits = freebits();
				final int freebits = freebits();
				if (m_freeBits > freebits)
					log.error("m_freeBits too high: " + m_freeBits + " > (calc): " + freebits);
				
				m_freeTransients = transientbits();
				
				checkFreeList();
				
				// assert m_sessionFrees == releasedAllocations : "Allocator: " + hashCode() + " m_sessionFrees: " + m_sessionFrees + " != released: " + releasedAllocations;
				if (start > releasedAllocations) {
					log.error("BAD! Allocator: " + hashCode() + ", size: " + m_size + " m_sessionFrees: " + m_sessionFrees.intValue() + " > released: " + releasedAllocations);
				} else {
					// log.error("GOOD! Allocator: " + hashCode() + ", size: " + m_size + " m_sessionFrees: " + m_sessionFrees.intValue() + " <= released: " + releasedAllocations);
				}
			// } finally {
				final int end = m_sessionFrees.getAndSet(0);
				assert start == end : "SessionFrees concurrent modification: " + start + " != " + end;
			// }
		} else {
			assert m_sessionFrees.intValue() == 0 : "Session Inactive with sessionFrees: " + m_sessionFrees.intValue();
		}
	}

	private int freebits() {
		int freeBits = 0;
		for (AllocBlock ab : m_allocBlocks) {
			freeBits += ab.freeBits();
		}

		return freeBits;
	}

	private int transientbits() {
		int freeBits = 0;
		for (AllocBlock ab : m_allocBlocks) {
			freeBits += ab.transientBits();
		}

		return freeBits;
	}

	public long getPhysicalAddress(final int offset) {
		return getPhysicalAddress(offset, false); // do NOT override address checks
	}

	void setAddressExternal(final int latchedAddr) {
		final int offset = ((-latchedAddr) & RWStore.OFFSET_BITS_MASK) - 3; // bit adjust
	
		final int nbits = 32 * m_bitSize;
		final int block = offset/nbits;
		final int bit = offset % nbits;
		
		final AllocBlock ab = m_allocBlocks.get(block);
		if (ab.m_addr == 0) {
			// fixup offset
			int blockSize = 32 * m_bitSize;
			blockSize *= m_size;
			blockSize >>= RWStore.ALLOCATION_SCALEUP;

			ab.m_addr = grabAllocation(m_store, blockSize);
			
			if (block == 0)
				m_startAddr = ab.m_addr;
		}
		
		ab.setBitExternal(bit);
		
		m_freeBits--;
	}

	public int getSlotSize() {
		return m_size;
	}
	
	/**
	 * Add the committed allocated slot contents to the digest
	 * 
	 * FIXME: First version is correct rather than optimal, need to
	 * consider if there is any benefit to 
	 * 
	 * @param snapshot
	 * @param digest
	 */
    public void computeDigest(final Object snapshot, final MessageDigest digest) {
    	// create buffer of slot size
    	final ByteBuffer bb = ByteBuffer.allocate(m_size);
    	final byte[] ba = m_index == 0 && s_islogDebug ? bb.array() : null;
    	
    	for (AllocBlock b : m_allocBlocks) {
    		final int bits = b.m_commit.length * 32;
    		final long startAddr = RWStore.convertAddr(b.m_addr);
    		for (int i = 0; i < bits; i++) {
    			if (RWStore.tstBit(b.m_commit, i)) {
    				final long paddr = startAddr + (m_size * i);
    				
    				bb.position(0);   				
                    m_store.readRaw(paddr, bb);
                    
                    digest.update(bb);

        			if (ba != null) {
        				log.debug(BytesUtil.toHexString(ba));
        			}
    			}
    		}
    	}
    	
		{
			final byte[] data = digest.digest();
			final StringBuffer sb = new StringBuffer();
			for (byte b : data) {
				if (sb.length() > 0)
					sb.append(",");
				sb.append(b);
			}

			log.warn("ALLOCATOR[" + m_index + ":" + m_size + "] freeBits: " + freebits() +  ", DIGEST:" + sb.toString());
			
		}
    }

    /**
     * Update the historical commit bits only once confirmed
     */
	public void postCommit() {
		final boolean protectTransients = m_sessionActive || m_store.isSessionProtected();
		
    	for (AllocBlock b : m_allocBlocks) {
            b.m_commit = b.m_live.clone();
            if (!protectTransients)
            	b.m_transients = b.m_live.clone();
            
            /**
             * If this allocator is shadowed then copy the new committed
             * state to m_saveCommit
             */
            if (m_context != null) {
            	// do not copy live bits to committed bits, leave to context.release()
                // throw new IllegalStateException("Must not commit shadowed FixedAllocator!");
//            } else if (m_store.isSessionPreserved()) {
//                block.m_commit = block.m_transients.clone();
            } else {
                b.m_commit = b.m_live.clone();
                // if m_saveCommit is set then it must be m_pendingContextCommit
                if (b.m_saveCommit != null) {
                	if (!m_pendingContextCommit)
                		throw new IllegalStateException("Unexpected m_saveCommit when no pending commit");
                	b.m_saveCommit = null;
                	b.m_isoFrees = null;
                }
            }

    	}
    	
		if (m_pendingContextCommit) {
			m_pendingContextCommit = false;
			if (m_freeWaiting && meetsSmallSlotThreshold()) {
				addToFreeList();
			}
		}
		
		if (!protectTransients /*!this.m_sessionActive*/) {
			m_freeBits += m_freeTransients;

			// Handle re-addition to free list once transient frees are
			// added back
			if (m_freeWaiting && meetsSmallSlotThreshold()) {
				addToFreeList();
			}

			m_freeTransients = 0;
			
		}
		
		if (s_islogDebug)
			checkBits();
		
	}

	/*
	 * Checks for allocations committed in xfa that are free in this allocator
	 * and should be removed from the historical external cache.
	 */
	public int removeFreedWrites(final FixedAllocator xfa,
			final ConcurrentWeakValueCache<Long, ICommitter> externalCache) {
		// Compare the committed bits in each AllocBlock
		int count = 0;
		for (int i = 0; i < m_allocBlocks.size(); i++) {
			final AllocBlock ab = m_allocBlocks.get(i);
			final AllocBlock xab = xfa.m_allocBlocks.get(i);
			// NOTE that absolute bit offsets are bumped by 3 for historical reasons
			final int blockBitOffset = 3 + (i * xab.m_commit.length * 32);
			for (int b = 0; b < xab.m_commit.length; b++) {
				if (xab.m_commit[b] != ab.m_commit[b]) { // some difference
					// compute those set in xfa not set in ab (removed)
					final int removed = xab.m_commit[b] & ~ab.m_commit[b];
					if (removed != 0) { // something to do
						// need to test each of 32 bits
						for (int bit = 0; bit < 32; bit++) {
							if ((removed & (1 << bit)) != 0) {
								// Test bit calculation
								final int tstBit = blockBitOffset + (b * 32) + bit;
								if (!(xfa.isCommitted(tstBit) && !isCommitted(tstBit)))  {
									log.error("Bit problem: " + tstBit);
								}
								
								final long paddr = xfa.getPhysicalAddress(tstBit);
								
								if (s_islogTrace) {
									log.trace("Checking address for removal: " + paddr);
								}
								
								count++;
								
								externalCache.remove(paddr);
							}
						}
					}
				}
			}
		}
		
		if (s_islogTrace)
			log.trace("FA index: " + m_index + ", freed: " + count);
		
		return count;
	}

	/**
	 * Determines if the provided physical address is within an allocated slot
	 * @param addr
	 * @return
	 */
	public boolean verifyAllocatedAddress(long addr) {
		if (s_islogTrace)
			log.trace("Checking Allocator " + m_index + ", size: " + m_size);

		final Iterator<AllocBlock> blocks = m_allocBlocks.iterator();
		final long range = m_size * m_bitSize * 32;
		while (blocks.hasNext()) {
			final int startAddr = blocks.next().m_addr;
			if (startAddr != 0) {
				final long start = RWStore.convertAddr(startAddr);
				final long end = start + range;
				
				if (s_islogTrace)
					log.trace("Checking " + addr + " between " + start + " - " + end);
				
				if (addr >= start && addr < end)
					return true;
			} else {
				break;
			}
		}
		return false;
	}

	/**
	 * Add a copy of the currently committed allocation data to the snapshot.  This is used by the snapshot
	 * mechanism to ensure that a file copy, taken over the course of multiple commits, will contain the
	 * correct allocation data from the time the snapshot was taken.
	 */
	void snapshot(final ISnapshotData tm) {
		if (m_diskAddr > 0)
			tm.put(m_store.metaBit2Addr(m_diskAddr), commitData());
	}
	
	/**
	 * Returns the 1K committed allocation data by writing the commit data for each allocation block.
	 */
	byte[] commitData() {
		try {			
			final byte[] buf = new byte[1024];
			final DataOutputStream str = new DataOutputStream(new FixedOutputStream(buf));
			try {
                str.writeInt(m_size);
                
                final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
                while (iter.hasNext()) {
                    final AllocBlock block = iter.next();

                    str.writeInt(block.m_addr);
                    for (int i = 0; i < m_bitSize; i++) {
                    	    str.writeInt(block.m_commit[i]);
                    }

                }
                // add checksum
                final int chk = ChecksumUtility.getCHK().checksum(buf,
                        str.size());
                str.writeInt(chk);
			} finally {
			    str.close();
			}

			return buf;
		} catch (IOException e) {
			throw new StorageTerminalError("Error on write", e);
		}
	}

	public void addToRegionMap(HashMap<Integer, FixedAllocator> map) {
		for (AllocBlock ab : m_allocBlocks) {
			if (ab.m_addr != 0) {
				final FixedAllocator pa = map.put(ab.m_addr, this);
				if (pa != null) {
					throw new IllegalStateException("Duplicate mapping Allocators, " + pa.m_index + ", " + m_index);
				}
			}
		}
	}

}
