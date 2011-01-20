/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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

package com.bigdata.rwstore.sector;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.bigdata.io.DirectBufferPool;
import com.bigdata.rwstore.FixedOutputStream;
import com.bigdata.rwstore.IWriteCacheManager;

/**
 * The SectorAllocator is designed as an alternative the the standard RWStore
 * FixedAllocators.
 * 
 * The idea of the SectorAllocator is to efficiently contain within a single
 * region as dense a usage as possible.  Since a SectorAllocator is able to
 * allocate a full range of slot sizes, it should be able to service several
 * thousand allocations and maximise disk locality on write.
 * 
 * Furthermore, it presents an option to be synced with the backing store -
 * similarly to a MappedFile, in which case a single write for the entire
 * sector could be made for update.
 * 
 * What we do not want is to run out of bits and to leave significant unused
 * space in the sector.  This could happen if we primarily allocated small
 * slots - say on average 512 bytes.  In this case, the maximum 1636 entries
 * would map 1636 * 32 * 512 bytes => only just over 26M, so a 64M sector
 * is massively wasteful.  The solution is to increment the sector reserve
 * as required for each tab, say by a minimum of 256K while ensuring always less
 * than 64M.  Bit waste in the allocator - where the sector memory is allocated
 * with far fewer bits than available is less of an issue, although it does
 * impact on teh total number of allocations available.  The absolute maximum
 * size of an RWStore is derived from the maximum sector size * the number
 * of sectors.  An 8K sector allows for 32K bits, which, which only requires
 * 15 unsigned bits, leaving a signed17bits for the sector index = 64K of
 * sectors.  Implying a maximum addressable store file of 64M * 64K,
 * = 4TB of full sectors.  If the average sector only requires 32M, then the
 * total store would be reduced appropriately.
 * 
 * The maximum theoretical storage is yielded by MAX_INT * AVG_SLOT_SIZE, so
 * 2GB * 2K (avg) would equate to the optimal maximum addressable allocations
 * and file size.  An AVG of > 2K yields fewer allocations and an AVG of < 2K
 * a reduced file size.
 * 
 * TODO: add parameterisation of META_SIZE for exploitation by MemoryManager.
 * TODO: cache block starts in m_addresses to simplify/optimise bit2Offset
 * 
 * @author Martyn Cutcher
 *
 */
public class SectorAllocator {
    protected static final Logger log = Logger
    .getLogger(SectorAllocator.class);

    static final int getBitMask(int bits) {
		int ret = 0;
		
		for (int i = 0; i < bits; i++) ret += 1 << i;
		
		return ret;
	}
	static final int SECTOR_INDEX_BITS = 16;
	static final int SECTOR_OFFSET_BITS = 32-SECTOR_INDEX_BITS;
	static final int SECTOR_OFFSET_MASK = getBitMask(SECTOR_OFFSET_BITS);

	static final int META_SIZE = 8192; // 8K
	
	final static int SECTOR_SIZE = 64 * 1024 * 1024; // 10M
	final static int NUM_ENTRIES = (META_SIZE - 12) / (4 + 1); // 8K - index - address / (4 + 1) bits plus tag
	final int[] BIT_MASKS = {0x1, 0x3, 0x7, 0xF, 0xFF, 0xFFFF, 0xFFFFFFFF};
	final static int BLOB_SIZE = 4096;
	final static int BLOB_CHAIN_OFFSET = BLOB_SIZE - 4;
	final static int[] ALLOC_SIZES = {64, 128, 256, 512, 1024, 2048, BLOB_SIZE};
	final static int[] ALLOC_BITS = {32, 32, 32, 32, 32, 32, 32};
	int m_index;
	long m_sectorAddress;
	int m_maxSectorSize;
	byte[] m_tags = new byte[NUM_ENTRIES];
	int[] m_bits = new int[NUM_ENTRIES]; // 128 - sectorAddress(1) - m_tags(4)

	int[] m_transientbits = new int[NUM_ENTRIES];
	int[] m_commitbits = new int[NUM_ENTRIES];
	int[] m_addresses = new int[NUM_ENTRIES];
	
	// maintain count against each alloc size, this provides ready access to be
	//	able to check the minimum number of bits for all tag sizes.  No
	//	SectorAllocator should be on the free list unless there are free bits
	//	available for all tags.
	//
	// In order to return a SectorAllocator to the free list we can check not
	//	only the total number of bits, but the average number of bits for the
	//	tag, dividing the numebr of free bits by the total (number of blocks)
	//	for each tag.
	int[] m_free = new int[ALLOC_SIZES.length];
	int[] m_total = new int[ALLOC_SIZES.length];
	int[] m_allocations = new int[ALLOC_SIZES.length];
	int[] m_recycles = new int[ALLOC_SIZES.length];
	
	final ISectorManager m_store;
	boolean m_onFreeList = false;
	private long m_diskAddr;
	private final IWriteCacheManager m_writes;

	public SectorAllocator(ISectorManager store, IWriteCacheManager writes) {
		m_store = store;
		m_writes = writes;
	}

	/**
	 * Must find tag type that size fits in (or BLOB) and then find
	 * block of type into which an allocation can be made.
	 * 
	 * 
	 */
	public int alloc(int size) {
		if (size > BLOB_SIZE) {
			throw new IllegalArgumentException("Cannot directly allocate a BLOB, use PSOutputStream");
		}
		
		if (!m_onFreeList)
			throw new IllegalStateException("Allocation request to allocator " + m_index + " not on the free list");
		
		byte tag = 0;
		while (size > ALLOC_SIZES[tag]) tag++;
		
		assert m_free[tag] > 0;

		// now find allocated tag areas..
		int sbit = 0;
		int lbits = 0;
		for (int i = 0; i < NUM_ENTRIES; i++) {
			final int ttag = m_tags[i];
			
			if (ttag == -1) {
				throw new IllegalStateException("Allocator should not be on the FreeList for tag: " + ALLOC_SIZES[tag]);
			}
			
			lbits = ALLOC_BITS[ttag];
			
			if (ttag == tag) {
				final int bits = m_transientbits[i];
				int bit = fndBit(bits);
				
				if (bit != -1) {
					sbit += bit;
					
					if (log.isTraceEnabled())
						log.trace("Setting bit: " + sbit);
					
					setBit(m_bits, sbit);
					setBit(m_transientbits, sbit);
					
					if (!tstBit(m_bits, sbit)) {
						throw new IllegalStateException("WTF with bit:" + sbit);
					}
					m_free[tag]--;
					
					m_allocations[tag]++;
					
					if (m_free[tag] == 0) {
						if (!addNewTag(tag)) {
							m_store.removeFromFreeList(this);
							m_onFreeList = false;
						}
					}
					
					int raddr = makeAddr(m_index, sbit);
					
					if (log.isTraceEnabled())
						log.trace("Allocating " + m_index + ":" + sbit + " as " + raddr + " for " + size);

					if (getSectorIndex(raddr) != m_index) {
						throw new IllegalStateException("Address: " + raddr + " does not yield index: " + m_index);
					}

					return raddr;
				}
			}
			sbit += lbits; // bump over current tag's bits
		}
		
		return 0;
	}
	
	public static int makeAddr(final int index, final int bit) {
		return -((index << SECTOR_OFFSET_BITS) + bit);
	}

	private boolean addNewTag(byte tag) {
		int allocated = 0;
		for (int i = 0; i < m_tags.length; i++) {
			if (m_tags[i] == -1) {
				int block = this.ALLOC_SIZES[tag] * 32;
				if ((allocated + block) <= m_maxSectorSize) {
					m_tags[i] = tag;
					m_free[tag] += 32;
					m_total[tag]++;
					if (log.isTraceEnabled())
						log.trace("addnewTag block for: " + ALLOC_SIZES[tag]);
					if ((i+1) == m_tags.length) {
						int trim = m_maxSectorSize - (allocated + block);

						m_store.trimSector(trim, this);
					}
					return true;
				} else {
					if (log.isDebugEnabled())
						log.debug("addnewTag FALSE due to Sector SIZE");
					
					return false;
				}
			} else {
				allocated += ALLOC_SIZES[m_tags[i]] * 32;
			}
		}
		
		if (log.isDebugEnabled())
			log.debug("addnewTag FALSE due to Sector BITS");
		return false;
	}

	/**
	 * 
	 * @param bit
	 * @return
	 */
	public boolean free(final int bit) {
		if (!tstBit(m_bits, bit)) {
			throw new IllegalStateException("Request to free bit not set: " + bit);
		}
		
		clrBit(m_bits, bit);
		if (!tstBit(m_commitbits, bit)) {
			if (!tstBit(m_transientbits, bit)) {
				throw new IllegalStateException("Request to free transient bit not set" + bit);
			}
			
			clrBit(m_transientbits, bit);
			int tag = bit2tag(bit);
			m_free[tag]++;
			
			m_recycles[tag]++;
			
			// The hasFree test is too coarse, ideally we should test for
			//	percentage of free bits - say 10% PLUS a minimum of say 10
			//	for each tag type.
			if ((!m_onFreeList) && hasFree(5)) { // minimum of 10 bits for all tags
				m_onFreeList = true;
				m_store.addToFreeList(this);
			}
			
			if (m_writes != null && m_writes.removeWriteToAddr(getPhysicalAddress(bit))) {
				if (log.isTraceEnabled())
					log.trace("Removed potential DUPLICATE");
			}
		}
		
		return false;
	}

	/**
	 * 
	 * @param bit
	 * @return the block size
	 */
	int bit2Size(int bit) {
		for (int t = 0; t < NUM_ENTRIES; t++) {
			int tag = m_tags[t];
			if (tag == -1) {
				throw new IllegalStateException("bit offset too large");
			}
			int bits = ALLOC_BITS[tag];
			if (bit < bits) {
				return ALLOC_SIZES[tag];
			}
			bit -= bits;
		}
		
		return 0;
	}
	/**
	 * 
	 * @param bit
	 * @return the offset in the sector
	 */
	int bit2Offset(int bit) {
		int offset = 0;
		for (int t = 0; t < NUM_ENTRIES; t++) {
			int tag = m_tags[t];
			if (tag == -1) {
				throw new IllegalStateException("bit offset too large");
			}
			int bits = ALLOC_BITS[tag];
			if (bit < bits) {
				offset += ALLOC_SIZES[tag] * bit;
				return offset;
			} else {
				offset += ALLOC_SIZES[tag] * bits;
				bit -= bits;
			}
		}
		
		return 0;
	}
	/**
	 * Since we know that all allocations are 32 bits each, there is no need to
	 * san through the array.
	 * 
	 * @param bit
	 * @return the tag of the bit
	 */
	public int bit2tag(int bit) {
		return m_tags[bit/32];
	}
	/**
	 * 
	 */
	public long getPhysicalAddress(int offset) {
		return m_sectorAddress + bit2Offset(offset);
	}

	public int getPhysicalSize(int offset) {
		return bit2Size(offset);
	}

	public long getStartAddr() {
		return m_sectorAddress;
	}

	public String getStats() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean hasFree(int threshold) {
		for (int i = 0; i < m_free.length; i++) {
			if ((m_free[i]/m_total[i]) < threshold)
				return false;
		}
		return true;
	}

	/**
	 * Checks
	 * @return if there is a positive free count for all tags
	 */
	public boolean hasFree() {
		return hasFree(1);
	}

	public void preserveSessionData() {
		// TODO Auto-generated method stub

	}

	public void read(DataInputStream str) {
		try {
			m_index = str.readInt();
			m_sectorAddress = str.readLong();
			int taglen = str.read(m_tags);
			assert taglen == m_tags.length;
			
			for (int i = 0; i < NUM_ENTRIES; i++) {
				m_bits[i] = str.readInt();
			}
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}

	public long getDiskAddr() {
		return m_diskAddr;
	}
	
	public void setDiskAddr(long addr) {
		m_diskAddr = addr;
	}

	public boolean verify(int addr) {
		// TODO Auto-generated method stub
		return false;
	}

	public byte[] write() {
		byte[] buf = new byte[META_SIZE];
		DataOutputStream str = new DataOutputStream(new FixedOutputStream(buf));
		try {
			str.writeInt(m_index);
			str.writeLong(m_sectorAddress);
			str.write(m_tags);
			for (int i = 0; i < NUM_ENTRIES; i++) {
				str.writeInt(m_bits[i]);
			}
			
			m_transientbits = (int[]) m_bits.clone();
			m_commitbits = (int[]) m_bits.clone();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				str.close();
			} catch (IOException e) {
				// ignore
			}
		}
		
		return buf;
	}

	public int addressSize(int offset) {
		return bit2Size(offset);
	}

	public void setIndex(int index) {
		assert m_index == 0;
		
		m_index = index;
	}

	public void addAddresses(final ArrayList<Long> addrs) {
		addrs.add(Long.valueOf(m_sectorAddress));
	}

	static void clrBit(int[] bits, int bitnum) {
		int index = bitnum / 32;
		int bit = bitnum % 32;

		int val = bits[index];

		val &= ~(1 << bit);

		bits[index] = val;
	}
	
	static void setBit(int[] bits, int bitnum) {
		int index = bitnum / 32;
		int bit = bitnum % 32;

		bits[index] |= 1 << bit;
	}

	static boolean tstBit(int[] bits, int bitnum) {
		int index = bitnum / 32;
		int bit = bitnum % 32;

		return (bits[index] & 1 << bit) != 0;
	}

	/*
	 * use divide and conquer rather than shifting through
	 */
	int fndBit(int bits) {
		for (int n = 0; n < 8; n++) { // check nibbles
			if ((bits & 0x0F) != 0xF) {
				for (int b = 0; b < 4; b++) {
					if ((bits & (1 << b)) == 0) {
						return b + (n * 4);
					}
				}
			}
			bits >>>= 4; //right shift a nibble
		}

		return -1;
	}

	/**
	 * As well as setting the address, this is the point when the
	 * allocator can pre-allocate the first set of tags.
	 * 
	 * @param sectorAddress managed by this Allocator
	 */
	public void setSectorAddress(final long sectorAddress, final int maxsize) {
		if (log.isInfoEnabled())
			log.info("setting sector address: " + sectorAddress);
		
		m_sectorAddress = sectorAddress;
		m_maxSectorSize = maxsize;
		for (int i = 0; i < ALLOC_SIZES.length; i++) {
			m_tags[i] = (byte) i;
			m_free[i] = 32;
			m_total[i] = 1;
		}
		for (int i = ALLOC_SIZES.length; i < NUM_ENTRIES; i++) {
			m_tags[i] = (byte) -1;
		}
		
		m_onFreeList = true;
		m_store.addToFreeList(this);
	}

	public static int getSectorIndex(int rwaddr) {
		return (-rwaddr) >> SECTOR_OFFSET_BITS;
	}

	public static int getSectorOffset(int rwaddr) {
		return (-rwaddr) & SECTOR_OFFSET_MASK;
	}

	public static int getBlobBlockCount(int size) {
		final int nblocks = (size + BLOB_SIZE - 1) / BLOB_SIZE;
		
		return nblocks;
	}

}
