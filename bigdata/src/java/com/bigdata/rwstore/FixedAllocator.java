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

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.io.*;

import org.apache.log4j.Logger;

import com.bigdata.rwstore.RWStore.AllocationStats;
import com.bigdata.rwstore.StorageStats.Bucket;
import com.bigdata.util.ChecksumUtility;

/**
 * FixedAllocator
 * 
 * Maintains List of AllocBlock(s)
 */
public class FixedAllocator implements Allocator {
    
    private static final Logger log = Logger.getLogger(FixedAllocator.class);

//	final private RWWriteCacheService m_writeCache;
	volatile private int m_freeBits;
	volatile private int m_freeTransients;

    /**
     * Address of the {@link FixedAllocator} within the meta allocation space on
     * the disk.
     */
	volatile private int m_diskAddr;
	volatile private int m_index;
	
	Bucket m_statsBucket = null;

	public void setIndex(final int index) {
		final AllocBlock fb = (AllocBlock) m_allocBlocks.get(0);
		
        if (log.isDebugEnabled())
            log.debug("Restored index " + index + " with " + getStartAddr()
                    + "[" + fb.m_live[0] + "] from " + m_diskAddr);

		m_index = index;
	}

	public long getStartAddr() {
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

	public int getDiskAddr() {
		return m_diskAddr;
	}

	public void setDiskAddr(final int addr) {
		m_diskAddr = addr;
	}

    /**
     * The tweak of 3 to the offset is to ensure 1, that no address is zero and
     * 2 to enable the values 1 & 2 to be special cased (this aspect is now
     * historical).
     */
	public long getPhysicalAddress(int offset) {
	  	offset -= 3;

		final int allocBlockRange = 32 * m_bitSize;

		final AllocBlock block = (AllocBlock) m_allocBlocks.get(offset / allocBlockRange);
		
		final int bit = offset % allocBlockRange;
		
		if (RWStore.tstBit(block.m_live, bit)) {		
			return RWStore.convertAddr(block.m_addr) + ((long) m_size * bit);
		} else {
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

	public void setFreeList(ArrayList list) {
		m_freeList = list;

		if (hasFree()) {
			m_freeList.add(this);
			m_freeWaiting = false;
		}
	}

	volatile private IAllocationContext m_context;
	public void setAllocationContext(final IAllocationContext context) {
		if (context == null && m_context != null) {
			// restore commit bits in AllocBlocks
			for (AllocBlock allocBlock : m_allocBlocks) {
				allocBlock.deshadow();
			}
		} else if (context != null & m_context == null) {
			// restore commit bits in AllocBlocks
			for (AllocBlock allocBlock : m_allocBlocks) {
				allocBlock.shadow();
			}
		}
		m_context = context;
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
			if (log.isDebugEnabled())
				log.debug("writing allocator " + m_index + " for " + getStartAddr() + " with " + fb.m_live[0]);
			final byte[] buf = new byte[1024];
			final DataOutputStream str = new DataOutputStream(new FixedOutputStream(buf));
			try {
                str.writeInt(m_size);

                final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
                while (iter.hasNext()) {
                    final AllocBlock block = iter.next();

                    str.writeInt(block.m_addr);
                    for (int i = 0; i < m_bitSize; i++) {
                        str.writeInt(block.m_live[i]);
                    }

//                    if (!m_store.isSessionPreserved()) {
                        block.m_transients = block.m_live.clone();
//                    }

                    /**
                     * If this allocator is shadowed then copy the new committed
                     * state to m_saveCommit
                     */
                    if (m_context != null) {
                        assert block.m_saveCommit != null;

                        block.m_saveCommit = block.m_live.clone();
//                    } else if (m_store.isSessionPreserved()) {
//                        block.m_commit = block.m_transients.clone();
                    } else {
                        block.m_commit = block.m_live.clone();
                    }
                }
                // add checksum
                final int chk = ChecksumUtility.getCHK().checksum(buf,
                        str.size());
                str.writeInt(chk);
			} finally {
			    str.close();
			}

//			if (!m_store.isSessionPreserved()) {
			m_freeBits += m_freeTransients;

			// Handle re-addition to free list once transient frees are
			// added back
			if ((m_freeTransients == m_freeBits) && (m_freeTransients != 0)) {
				m_freeList.add(this);
				m_freeWaiting = false;
			}

			m_freeTransients = 0;
//			}

			return buf;
		} catch (IOException e) {
			throw new StorageTerminalError("Error on write", e);
		}
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
						for (int bit = 0; bit < 32; bit++) {
							if ((anInt & (1 << bit)) == 0) {
								m_freeBits++;
							}
						}
					}
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
    private final int m_size;

	private int m_startAddr = 0;
	private int m_endAddr = 0;

	/**
	 * The #of ints in the {@link AllocBlock}'s internal arrays.
	 */
	private final int m_bitSize;

	private final ArrayList<AllocBlock> m_allocBlocks;

	final private RWStore m_store;

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
		m_diskAddr = 0;
		m_store = store;

		m_size = size;

		/*
		 * For smaller allocations we'll allocate a larger span, this is needed
		 * to ensure the minimum allocation is large enough to guarantee 
		 * a unique address for a BlobAllocator.
		 */
		if (m_size < 256) {
			/*
			 * Note: 64 ints is 256 bytes is 2048 bits, so 2048 allocation
			 * slots.
			 */ 
			m_bitSize = 64;
		} else {
			/*
			 * Note: 32 ints is 128 bytes is 1024 bits, so 1024 allocation
			 * slots.
			 */ 
			m_bitSize = 32;
		}

//		m_writeCache = cache;

		// number of blocks in this allocator, bitSize plus 1 for start address
		final int numBlocks = 255 / (m_bitSize + 1);

		/*
		 * Create AllocBlocks for this FixedAllocator, but do not allocate
		 * either the AllocBlocks or their managed allocation slots on the
		 * persistent heap yet.
		 */
		m_allocBlocks = new ArrayList<AllocBlock>(numBlocks);
		for (int i = 0; i < numBlocks; i++) {
			m_allocBlocks.add(new AllocBlock(0, m_bitSize));//, cache));
		}

		m_freeTransients = 0;
		m_freeBits = 32 * m_bitSize * numBlocks;
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
	
	public boolean free(final int addr, final int size) {
		if (addr < 0) {
			final int offset = ((-addr) & RWStore.OFFSET_BITS_MASK) - 3; // bit adjust

			final int nbits = 32 * m_bitSize;

			final int block = offset/nbits;
			
			if (((AllocBlock) m_allocBlocks.get(block))
					.freeBit(offset % nbits)) { // bit adjust
				
				// Only add back to the free list if at least 3000 bits avail
				if (m_freeBits++ == 0 && false) {
					m_freeWaiting = false;
					m_freeList.add(this);
				} else if (m_freeWaiting && m_freeBits == m_store.cDefaultFreeBitsThreshold) {
					m_freeWaiting = false;
					m_freeList.add(this);
				}
			} else {
				m_freeTransients++;
			}
			
			if (m_statsBucket != null) {
				m_statsBucket.delete(size);
			}

			return true;
		} else if (addr >= m_startAddr && addr < m_endAddr) {

			final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
			while (iter.hasNext()) {
				final AllocBlock block = iter.next();
				if (block.free(addr, m_size)) {
					m_freeTransients++;

					return true;
				}
			}
		}
		
		return false;
	}

	/**
	 * The introduction of IAllocationContexts has added some complexity to
	 * the older concept of a free list.  With AllocationContexts it is
	 * possibly for allocator to have free space available but this being
	 * restricted to a specific AllocaitonContext.  The RWStore alloc method
	 * must therefore handle the 
	 */
	public int alloc(final RWStore store, final int size, final IAllocationContext context) {

        if (size <= 0)
            throw new IllegalArgumentException(
                    "Allocate requires positive size, got: " + size);

	    int addr = -1;

		final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
		int count = -1;
		while (addr == -1 && iter.hasNext()) {
			count++;

			final AllocBlock block = iter.next();
			if (block.m_addr == 0) {
				int blockSize = 32 * m_bitSize;
				if (m_statsBucket != null) {
					m_statsBucket.addSlots(blockSize);
				}
				blockSize *= m_size;
				blockSize >>= RWStore.ALLOCATION_SCALEUP;

				block.m_addr = store.allocBlock(blockSize);
				if (log.isInfoEnabled())
					log.info("Allocation block at " + block.m_addr + " of " + (blockSize << 16) + " bytes");

				if (m_startAddr == 0) {
					m_startAddr = block.m_addr;
				}
				m_endAddr = block.m_addr - blockSize;
			}
			addr = block.alloc(m_size);
		}

		if (addr != -1) {
	    	addr += 3; // Tweak to ensure non-zero address for offset 0

	    	if (--m_freeBits == 0) {
	    		if (log.isTraceEnabled())
	    			log.trace("Remove from free list");
				m_freeList.remove(this);
				m_freeWaiting = true;

				// Should have been first on list, now check for first
				if (m_freeList.size() > 0) {
					final FixedAllocator nxt = (FixedAllocator) m_freeList.get(0);
                    if (log.isInfoEnabled())
                        log.info("Freelist head: " + nxt.getSummaryStats());
				}
			}

			addr += (count * 32 * m_bitSize);

			final int value = -((m_index << RWStore.OFFSET_BITS) + addr);
			
			if (m_statsBucket != null) {
				m_statsBucket.allocate(size);
			}

			return value;
		} else {
    		if (log.isTraceEnabled())
    			log.trace("FixedAllocator returning null address");
    		
			return 0;
		}
	}

	public boolean hasFree() {
		return m_freeBits > 0;
	}

	public void addAddresses(ArrayList addrs) {
		
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

	/**
	 * If the context is this allocators context AND it is not in the commit bits
	 * then we can immediately free.
	 */
    public boolean canImmediatelyFree(final int addr, final int size,
            final IAllocationContext context) {
		if (context == m_context) {
			final int offset = ((-addr) & RWStore.OFFSET_BITS_MASK); // bit adjust

			return !isCommitted(offset);
		} else {
			return false;
		}
	}

	public void setBucketStats(Bucket b) {
		m_statsBucket = b;
	}
}
