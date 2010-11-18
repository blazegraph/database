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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;

/**
 * Maintains stats on the RWStore allocations, useful for tuning Allocator
 * sizes and tracking store efficiency.
 * 
 * It can also track reallocation patterns that are lost in static snapshots
 * of current usage.
 * 
 * Stats are retained on external requests and also on internal allocator
 * use.
 * 
 * totalSlots represents the total reserved slots in all the AllocationBlocks
 * for the associated FixedAllocators.
 * 
 * slotAllocations is the total of all allocations made
 * 
 * slotDeletes is the total of all slot deletions
 * 
 * Therefore the total of currently allocated slots is 
 * 		slotAllocations - slotDeletes
 * 
 * sizeAllocations is the total in bytes of the actual data stored in the
 * slots
 * 
 * sizeDeletes is the total in bytes of the actual data that has been deleted
 * 
 * Therefore the size of the total in bytes stored in currently allocated slots is
 * 		sizeAllocations - sizeDeletes
 * 
 * @author Martyn Cutcher
 *
 */
public class StorageStats {
	final int m_maxFixed;
	
	public class BlobBucket {
		final int m_size;
		long m_allocations;
		long m_deletes;
		
		public BlobBucket(final int size) {
			m_size = size;
		}
		public BlobBucket(DataInputStream instr) throws IOException {
			m_size = instr.readInt();
			m_allocations = instr.readLong();
			m_deletes = instr.readLong();
		}
		public void write(DataOutputStream outstr) throws IOException {
			outstr.writeInt(m_size);
			outstr.writeLong(m_allocations);
			outstr.writeLong(m_deletes);
		}
		public void delete() {
			m_deletes++;
		}
		public void allocate() {
			m_allocations++;
		}
	}
	
	public class Bucket {
		final int m_size;
		int m_allocators;
		long m_totalSlots;
		long m_slotAllocations;
		long m_slotDeletes;
		long m_sizeAllocations;
		long m_sizeDeletes;
		
		public Bucket(final int size) {
			m_size = size;
		}
		public Bucket(DataInputStream instr) throws IOException {
			m_size = instr.readInt();
			m_allocators = instr.readInt();
			m_slotAllocations = instr.readLong();
			m_slotDeletes = instr.readLong();
			m_totalSlots = instr.readLong();
			m_sizeAllocations = instr.readLong();
			m_sizeDeletes = instr.readLong();
		}
		public void write(DataOutputStream outstr) throws IOException {
			outstr.writeInt(m_size);
			outstr.writeInt(m_allocators);
			outstr.writeLong(m_slotAllocations);
			outstr.writeLong(m_slotDeletes);
			outstr.writeLong(m_totalSlots);
			outstr.writeLong(m_sizeAllocations);
			outstr.writeLong(m_sizeDeletes);
		}
		public void delete(int sze) {
			if (sze < 0)
				throw new IllegalArgumentException("delete requires positive size, got: " + sze);
			
			if (sze > m_size) {
				// sze = ((sze - 1 + m_maxFixed)/ m_maxFixed) * 4; // Blob header
				
				throw new IllegalArgumentException("Deletion of address with size greater than slot - " + sze + " > " + m_size);
			}
			
			m_sizeDeletes += sze;
			m_slotDeletes++;
		}
		public void allocate(int sze) {
			if (sze <= 0)
				throw new IllegalArgumentException("allocate requires positive size, got: " + sze);
			
			m_sizeAllocations += sze;
			m_slotAllocations++;
		}
		
		public void addSlots(int slots) {
			m_totalSlots += slots;
		}
		
		public long usedSlots() {
			return m_slotAllocations - m_slotDeletes;
		}
		
		public long usedStore() {
			return m_sizeAllocations - m_sizeDeletes;
		}
		
		// return as percentage
		public float slotWaste() {	
			if (usedStore() == 0)
				return 0.0f;
			
			BigDecimal size = new BigDecimal(m_size * usedSlots());
			BigDecimal store = new BigDecimal(100 * usedStore());
			store = store.divide(size, 2, RoundingMode.HALF_UP);
			BigDecimal total = new BigDecimal(100);
			
			return total.subtract(store).floatValue();
		}
		public float totalWaste() {	
			if (usedStore() == 0)
				return 0.0f;
			
			BigDecimal size = new BigDecimal(m_size * m_totalSlots);
			BigDecimal store = new BigDecimal(100 * usedStore());			
			store = store.divide(size, 2, RoundingMode.HALF_UP);			
			BigDecimal total = new BigDecimal(100);
			
			return total.subtract(store).floatValue();
		}
		public long reservedStore() {
			return m_size * m_totalSlots;
		}
		public void addAlocator() {
			m_allocators++;
		}
	}
	
	final ArrayList<Bucket> m_buckets;
	final ArrayList<BlobBucket> m_blobBuckets;
	
	// store total bytes allocated/deleted as blobs
	long m_blobAllocation;
	long m_blobDeletion;
	
	/**
	 * 
	 * @param buckets - the slot sizes used by the FixedAllocators
	 */
	public StorageStats(final int[] buckets) {
		m_buckets = new ArrayList<Bucket>();
		for (int i = 0; i < buckets.length; i++) {
			m_buckets.add(new Bucket(buckets[i]*64)); // slot sizes are 64 multiples
		}
		// last fixed allocator needed to compute BlobBuckets
		m_maxFixed = m_buckets.get(buckets.length-1).m_size;
		m_blobBuckets = new ArrayList<BlobBucket>();
		int curInc = m_maxFixed;
		int nxtBlob = m_maxFixed;
		final int cMaxBucket = 64 * 1024 * 1024; // 64 Mb
		while (nxtBlob < cMaxBucket) {
			nxtBlob += curInc;
			m_blobBuckets.add(new BlobBucket(nxtBlob));
			curInc *= 2;
		}
		m_blobBuckets.add(new BlobBucket(Integer.MAX_VALUE)); // catch all
	}
	
	/**
	 * 
	 * @param instr restore from reopen
	 * 
	 * @throws IOException
	 */
	public StorageStats(final DataInputStream instr) throws IOException {
		m_buckets = new ArrayList<Bucket>();
		int nbuckets = instr.readInt();
		for (int i = 0; i < nbuckets; i++) {
			m_buckets.add(new Bucket(instr));
		}
		m_maxFixed = m_buckets.get(m_buckets.size()-1).m_size;
		m_blobBuckets = new ArrayList<BlobBucket>();
		int nblobbuckets = instr.readInt();
		for (int i = 0; i < nblobbuckets; i++) {
			m_blobBuckets.add(new BlobBucket(instr));
		}
		m_blobAllocation = instr.readLong();
		m_blobDeletion = instr.readLong();
	}
	
	public byte[] getData() throws IOException {
		ByteArrayOutputStream outb = new ByteArrayOutputStream();
		DataOutputStream outd = new DataOutputStream(outb);
		
		outd.writeInt(m_buckets.size());
		
		for (Bucket b : m_buckets) {
			b.write(outd);
		}
		
		outd.writeInt(m_blobBuckets.size());
		
		for (BlobBucket b : m_blobBuckets) {
			b.write(outd);
		}
		
		outd.writeLong(m_blobAllocation);
		outd.writeLong(m_blobDeletion);
		
		outd.flush();
		
		return outb.toByteArray();
	}
	
	public void allocateBlob(int sze) {
		m_blobAllocation += sze;
		
		// increment blob bucket
		findBlobBucket(sze).allocate();
	}
	
	public void deleteBlob(int sze) {
		m_blobDeletion -= sze;
		
		// decrement blob bucket
		findBlobBucket(sze).delete();
	}
	
	private BlobBucket findBlobBucket(final int sze) {
		for (BlobBucket b : m_blobBuckets) {
			if (sze < b.m_size)
				return b;
		}
		
		throw new IllegalStateException("BlobBuckets have not been correctly set");
	}
	
	public void register(FixedAllocator alloc, boolean init) {
		int block = alloc.getBlockSize();
		for (Bucket b : m_buckets) {
			if (b.m_size == block) {
				alloc.setBucketStats(b);
				if (init)
					b.addAlocator();
				return;
			}
		}
		
		throw new IllegalArgumentException("FixedAllocator with unexpected block size");
	}
	
	public void register(FixedAllocator alloc) {
		register(alloc, false);
	}
	
	public void showStats(StringBuilder str) {
		str.append("\n-------------------------\n");
		str.append("RWStore Allocator Summary\n");
		str.append("-------------------------\n");
		str.append(padRight("AllocatorSize", 16));
		str.append(padLeft("AllocatorCount", 16));
		str.append(padLeft("SlotsAllocated", 16));
		str.append(padLeft("SlotsRecycled", 16));
		str.append(padLeft("SlotsInUse", 16));
		str.append(padLeft("SlotsReserved", 16));
		str.append(padLeft("BytesReserved", 16));
		str.append(padLeft("BytesAppData", 16));
		str.append(padLeft("%SlotWaste", 16));
		str.append(padLeft("%StoreWaste", 16));
		str.append(padLeft("%AppData", 16));
		str.append(padLeft("%StoreFile", 16));
		str.append("\n");
		
		long totalAppData = 0;
		long totalFileStore = 0;
		for (Bucket b: m_buckets) {
			totalAppData += b.usedStore();
			totalFileStore += b.reservedStore();
		}
		for (Bucket b: m_buckets) {
			str.append(padRight("" + b.m_size, 16));
			str.append(padLeft("" + b.m_allocators, 16));
			str.append(padLeft("" + b.m_slotAllocations, 16));
			str.append(padLeft("" + b.m_slotDeletes, 16));
			str.append(padLeft("" + b.usedSlots(), 16));
			str.append(padLeft("" + b.m_totalSlots, 16));
			str.append(padLeft("" + b.reservedStore(), 16));
			str.append(padLeft("" + b.usedStore(), 16));
			str.append(padLeft("" + b.slotWaste() + "%", 16));
			str.append(padLeft("" + b.totalWaste() + "%", 16));
			str.append(padLeft("" + dataPercent(b.usedStore(), totalAppData) + "%", 16));
			str.append(padLeft("" + dataPercent(b.reservedStore(), totalFileStore) + "%", 16));
			str.append("\n");
		}
		
		str.append("\n-------------------------\n");
		str.append("BLOBS\n");
		str.append("-------------------------\n");
		str.append(padRight("Bucket", 10));
		str.append(padLeft("Allocations", 12));
		str.append(padLeft("Deletes", 12));
		str.append(padLeft("Current", 12));
		str.append("\n");

		for (BlobBucket b: m_blobBuckets) {
			str.append(padRight("" + (b.m_size/1024) + "K", 10));
			str.append(padLeft("" + b.m_allocations, 12));
			str.append(padLeft("" + b.m_deletes, 12));
			str.append(padLeft("" + (b.m_allocations - b.m_deletes), 12));
			str.append("\n");
		}
		
	}

	private float dataPercent(long usedData, long totalData) {
		BigDecimal used = new BigDecimal(100 * usedData);
		BigDecimal total = new BigDecimal(totalData);
		
		return used.divide(total, 2, RoundingMode.HALF_UP).floatValue();
	}

	public static String padLeft(String str, int minlen) {
		if (str.length() >= minlen)
			return str;
		
		StringBuffer out = new StringBuffer();
		int pad = minlen - str.length();
		while (pad-- > 0) {
			out.append(' ');
		}
		out.append(str);
		
		return out.toString();
	}
	
	public static String padRight(String str, int minlen) {
		if (str.length() >= minlen)
			return str;
		
		StringBuffer out = new StringBuffer();
		out.append(str);
		int pad = minlen - str.length();
		while (pad-- > 0) {
			out.append(' ');
		}
		
		return out.toString();
	}
}
