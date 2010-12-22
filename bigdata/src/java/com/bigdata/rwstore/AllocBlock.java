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

package com.bigdata.rwstore;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.bigdata.rwstore.RWStore.AllocationStats;

/**
 * Bit maps for an allocator. The allocator is a bit map managed as int[]s.
 * 
 * @todo change to make {@link #m_transients}, {@link #m_live}, and
 *       {@link #m_commit} final fields and then modify {@link FixedAllocator}
 *       to use {@link System#arraycopy(Object, int, Object, int, int)} to copy
 *       the data rather than cloning it.
 * 
 * @todo Review the locks held during reads against {@link AllocBlock}. Is it
 *       possible that we could have updates which are not being made visible to
 *       readers?
 * 
 * @todo change to use long[]s.
 */
public class AllocBlock {

    private static final Logger log = Logger.getLogger(AllocBlock.class);
    
    /**
	 * The FixedAllocator owning this block.  The callback reference is needed
	 * to allow the AllocBlock to determine the session state and whether to
	 * clear the transient bits.
	 */
	final FixedAllocator m_allocator;
	/**
	 * The address of the {@link AllocBlock} -or- ZERO (0) if {@link AllocBlock}
	 * has not yet been allocated on the persistent heap. Note that the space
	 * for the allocation slots managed by an {@link AllocBlock} is not reserved
	 * until the {@link AllocBlock} is allocated on the persistent heap.
	 */
	int m_addr;
	/**
	 * The dimension of the arrays, which is the #of allocation slots divided by
	 * 32 (sizeof(int)).
	 */
	private final int m_ints;
	/**
	 * The bits that were allocated in the previous commit. They can be freed in
	 * the current native transaction but they can not be reallocated until the
	 * next native transaction.
	 */
	int m_commit[];
	/**
	 * If used as a shadow allocator, then the _commit is saved to m_saveCommit
	 * and m_transients is copied to m_commit.
	 */
	int m_saveCommit[];
	/**
	 * Just the newly allocated bits. This will be copied onto {@link #m_commit}
	 * when the current native transaction commits.
	 */
	final int m_live[];
	/**
	 * All of the bits from the commit point on entry to the current native
	 * transaction plus any newly allocated bits.
	 */
	int m_transients[];
//	/**
//	 * Used to clear an address on the {@link WriteCacheService} if it has been
//	 * freed.
//	 */
//	private final RWWriteCacheService m_writeCache;

	AllocBlock(final int addrIsUnused, final int bitSize, final FixedAllocator allocator) {//, final RWWriteCacheService cache) {
//		m_writeCache = cache;
		m_allocator = allocator;
		m_ints = bitSize;
		m_commit = new int[bitSize];
		m_live = new int[bitSize];
		m_transients = new int[bitSize];
	}

	public boolean verify(final int addr, final int size) {
		if (addr < m_addr || addr >= (m_addr + (size * 32 * m_ints))) {
			return false;
		}

		// Now check to see if it allocated
		final int bit = (addr - m_addr) / size;

		return RWStore.tstBit(m_live, bit);
	}

	public boolean addressInRange(final int addr, final int size) {
		return (addr >= m_addr && addr <= (m_addr + (size * 32 * m_ints)));
	}

	public boolean free(final int addr, final int size) {
		if (addr < m_addr || addr >= (m_addr + (size * 32 * m_ints))) {
			return false;
		}

		freeBit((addr - m_addr) / size);

		return true;
	}

	public boolean freeBit(final int bit) {
		// by default do NOT session protect, the 2 argument call is made
		// directly from the RWStore that has access to sessio and transaction
		// state
		return freeBit(bit, false);
	}
	
	/*
	 * 
	 */
	public boolean freeBit(final int bit, final boolean sessionProtect) {
		if (!RWStore.tstBit(m_live, bit)) {
			throw new IllegalArgumentException("Freeing bit not set");
		}

		/*
		 * Allocation optimization - if bit NOT set in committed memory then
		 * clear the transient bit to permit reallocation within this
		 * transaction.
		 * 
		 * Note that with buffered IO there is also an opportunity to avoid
		 * output to the file by removing any pending write to the now freed
		 * address. On large transaction scopes this may be significant.
		 * 
		 * The sessionProtect parameter indicates whether we really should
		 * continue to protect this alloction by leaving the transient bit
		 * set.  For general session protection we should, BUT it allocation
		 * contexts have been used we can allow immediate recycling and this
		 * is setup by the caller
		 */
		RWStore.clrBit(m_live, bit);
		
		if (log.isTraceEnabled()) {
			log.trace("Freeing " + bitPhysicalAddress(bit) + " sessionProtect: " + sessionProtect);
		}

		if (!sessionProtect) {
			if (!RWStore.tstBit(m_commit, bit)) {
				RWStore.clrBit(m_transients, bit);
	
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}
	
	private long bitPhysicalAddress(int bit) {
		return RWStore.convertAddr(m_addr) + ((long) m_allocator.m_size * bit);
	}

	/**
	 * The shadow, if non-null defines the context for this request.
	 * 
	 * If an existing shadow is registered, then the allocation fails
	 * immediately.
	 * 
	 * If no existing shadow is registered, and a new allocation can be made
	 * then this AllocBlock is registered with the shadow.
	 * 
	 * Note that when shadows are used, an allocator on a free list may not have
	 * allocations available for all contexts, so the assumption that presence
	 * on the free list implies availability is not assertable.
	 */

	public int alloc(final int size) {
		if (size < 0) {
			throw new Error("Storage allocation error : negative size passed");
		}

		final int bit = RWStore.fndBit(m_transients, m_ints);

		if (bit != -1) {
			RWStore.setBit(m_live, bit);
			RWStore.setBit(m_transients, bit);

			return bit;
		} else {
			return -1;
		}
	}

	public boolean hasFree() {
		for (int i = 0; i < m_ints; i++) {
			if (m_live[i] != 0xFFFFFFFF) {
				return true;
			}
		}

		return false;
	}

	public int getAllocBits() {
		int total = m_ints * 32;
		int allocBits = 0;
		for (int i = 0; i < total; i++) {
			if (RWStore.tstBit(m_live, i)) {
				allocBits++;
			}
		}

		return allocBits;
	}

	public String getStats(final AllocationStats stats) {
		final int total = m_ints * 32;
		final int allocBits = getAllocBits();

		if (stats != null) {
			stats.m_reservedSlots += total;
			stats.m_filledSlots += allocBits;
			
			return "";
		}
		
		return " - start addr : " + RWStore.convertAddr(m_addr) + " [" + allocBits + "::" + total + "]";
	}

	public void addAddresses(final ArrayList addrs, final int rootAddr) {
		final int total = m_ints * 32;

		for (int i = 0; i < total; i++) {
			if (RWStore.tstBit(m_live, i)) {
				addrs.add(new Integer(rootAddr - i));
			}
		}
	}

	/**
	 * Store m_commit bits in m_saveCommit then duplicate transients to m_commit.
	 * 
	 * This ensures, that while shadowed, the allocator will not re-use storage
	 * that was allocated prior to the shadow creation.
	 */
	public void shadow() {
		m_saveCommit = m_commit;
		m_commit = m_transients.clone();
	}

	/**
	 * The transient bits will have been added to correctly, we now just need to
	 * restore the commit bits from the m_saveCommit, to allow re-allocation
	 * of non-committed storage.
	 */
	public void deshadow() {
		m_commit = m_saveCommit;
		m_saveCommit = null;
	}

	/**
	 * Must release allocations made by this allocator.
	 * 
	 * The commit bits are the old transient bits, so any allocated bits
	 * set in live, but not in commit, were set within this context.
	 * 
	 * The m_commit is the m_transients bits at the point of the
	 * link of the allocationContext with this allocator, bits set in m_live
	 * that are not set in m_commit, were made by this allocator for the
	 * aborted context.
	 * 
	 * L 1100	0110	AC	0111	AB	0110
	 * T 1100	1110		1111		1110	
	 * C 1100	1100		1110		1100
	 */
	public void abortshadow() {
		for (int i = 0; i < m_live.length; i++) {
			m_live[i] &= m_commit[i];
			m_transients[i] = m_live[i] | m_saveCommit[i];
		}
		m_commit = m_saveCommit;
	}

	/**
	 * When a session is active, the transient bits do not equate to an ORing
	 * of the committed bits and the live bits, but rather an ORing of the live
	 * with all the committed bits since the start of the session.
	 * When the session is released, the state is restored to an ORing of the
	 * live and the committed, thus releasing slots for re-allocation.
	 * 
	 * For each transient bit, check if cleared and ensure any write is removed
	 * from the write cache. Where the bit is set in the session protected
	 * but not in the recalculated transient.  Tested with new &= ~old;
	 * 
	 * @param cache 
	 * @return the number of allocations released
	 */
	public int releaseSession(RWWriteCacheService cache) {
		int freebits = 0;
		
		if (m_addr != 0) { // check active!
			for (int i = 0; i < m_live.length; i++) {
				int chkbits = m_transients[i];
				m_transients[i] = m_live[i] | m_commit[i];
				chkbits &= ~m_transients[i];
				
				final int startBit = i * 32;
				if (chkbits != 0) {
					// there are writes to clear
					for (int b = 0; b < 32; b++) {
						if ((chkbits & (1 << b)) != 0) {
							long clr = RWStore.convertAddr(m_addr) + ((long) m_allocator.m_size * (startBit + b));
							
							if (log.isTraceEnabled())
								log.trace("releasing address: " + clr);
							
							cache.clearWrite(clr);
							
							freebits++;
						}
					}
				}
			}
		}
		
		return freebits;
	}

	public String show() {
		StringBuilder sb = new StringBuilder();
		sb.append("AllocBlock, baseAddress: " + RWStore.convertAddr(m_addr) + " bits: ");
		for (int b: m_transients)
			sb.append(b + " ");
		
		return sb.toString();
	}

}
