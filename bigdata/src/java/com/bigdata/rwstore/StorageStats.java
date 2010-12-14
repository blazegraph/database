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
	final int cVersion = 0x0100;
	
	final int m_maxFixed;
	
	public class BlobBucket {
		final int m_size;
		long m_allocationSize;
		long m_allocations;
		long m_deletes;
		long m_deleteSize;
		
		public BlobBucket(final int size) {
			m_size = size;
		}
		public BlobBucket(DataInputStream instr) throws IOException {
			m_size = instr.readInt();
			m_allocationSize = instr.readLong();
			m_allocations = instr.readLong();
			m_deleteSize = instr.readLong();
			m_deletes = instr.readLong();
		}
		public void write(DataOutputStream outstr) throws IOException {
			outstr.writeInt(m_size);
			outstr.writeLong(m_allocationSize);
			outstr.writeLong(m_allocations);
			outstr.writeLong(m_deleteSize);
			outstr.writeLong(m_deletes);
		}
		public void delete(int sze) {
			m_deleteSize += sze;
			m_deletes++;
		}
		public void allocate(int sze) {
			m_allocationSize += sze;
			m_allocations++;
		}
		public long active() {
			return m_allocations - m_deletes;
		}
		public int meanAllocation() {
			if (m_allocations == 0)
				return 0;
			return (int) (m_allocationSize / m_allocations);
		}
		public float churn() {
			if (active() == 0)
				return m_allocations;
			
			BigDecimal allocs = new BigDecimal(m_allocations);
			BigDecimal used = new BigDecimal(active());			
			
			return allocs.divide(used, 2, RoundingMode.HALF_UP).floatValue();
		}
	}
	
	public class Bucket {
		final int m_start;
		final int m_size;
		int m_allocators;
		long m_totalSlots;
		long m_slotAllocations;
		long m_slotDeletes;
		long m_sizeAllocations;
		long m_sizeDeletes;
		
		public Bucket(final int size, final int startRange) {
			m_size = size;
			m_start = startRange;
		}
		public Bucket(DataInputStream instr) throws IOException {
			m_size = instr.readInt();
			m_start = instr.readInt();
			m_allocators = instr.readInt();
			m_slotAllocations = instr.readLong();
			m_slotDeletes = instr.readLong();
			m_totalSlots = instr.readLong();
			m_sizeAllocations = instr.readLong();
			m_sizeDeletes = instr.readLong();
		}
		public void write(DataOutputStream outstr) throws IOException {
			outstr.writeInt(m_size);
			outstr.writeInt(m_start);
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
			
			if (m_size > 64 && sze < 64) {
				// if called from deferFree then may not include size.  If so then use
				//	average size of slots to date as best running estimate.
				sze = meanAllocation();
			}
			
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
		
		public long emptySlots() {
			return m_totalSlots - usedSlots();
		}
		
		public long usedStore() {
			return m_sizeAllocations - m_sizeDeletes;
		}
		
		// return as percentage
		public float slotWaste() {	
			if (usedStore() == 0)
				return 0.0f;
			
			BigDecimal size = new BigDecimal(reservedStore());
			BigDecimal store = new BigDecimal(100 * (reservedStore() - usedStore()));
			if(store.signum()==0) return 0f;
			return store.divide(size, 2, RoundingMode.HALF_UP).floatValue();
		}
		public float totalWaste(long total) {	
			if (usedStore() == 0)
				return 0.0f;
			
			long slotWaste = reservedStore() - usedStore();
			
			BigDecimal localWaste = new BigDecimal(100 * slotWaste);
			BigDecimal totalWaste = new BigDecimal(total);			
			if(totalWaste.signum()==0) return 0f;
			return localWaste.divide(totalWaste, 2, RoundingMode.HALF_UP).floatValue();
		}
		public long reservedStore() {
			return m_size * m_totalSlots;
		}
		public void addAlocator() {
			m_allocators++;
		}
		public float slotChurn() {
			// Handle case where we may have deleted all allocations
			if (usedSlots() == 0)
				return m_slotAllocations;
			
			BigDecimal allocs = new BigDecimal(m_slotAllocations);
			BigDecimal used = new BigDecimal(usedSlots());			
			if(used.signum()==0) return 0f;
			return allocs.divide(used, 2, RoundingMode.HALF_UP).floatValue();
		}
		public float slotsUnused() {
			BigDecimal used = new BigDecimal(100 * (m_totalSlots-usedSlots()));			
			BigDecimal total = new BigDecimal(m_totalSlots);
			if(total.signum()==0) return 0f;
			return used.divide(total, 2, RoundingMode.HALF_UP).floatValue();
		}
		public float percentAllocations(long totalAllocations) {
			BigDecimal used = new BigDecimal(100 * m_slotAllocations);			
			BigDecimal total = new BigDecimal(totalAllocations);
			if(total.signum()==0) return 0f;
			return used.divide(total, 2, RoundingMode.HALF_UP).floatValue();
		}
		public float percentSlotsInuse(long totalInuse) {
			BigDecimal used = new BigDecimal(100 * usedSlots());			
			BigDecimal total = new BigDecimal(totalInuse);
			if(total.signum()==0) return 0f;
			return used.divide(total, 2, RoundingMode.HALF_UP).floatValue();
		}
		public int meanAllocation() {
			if (m_slotAllocations == 0)
				return 0;
			
			return (int) (m_sizeAllocations / m_slotAllocations);
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
		int prevLimit = 0;
		for (int i = 0; i < buckets.length; i++) {
			m_buckets.add(new Bucket(buckets[i]*64, prevLimit)); // slot sizes are 64 multiples
			prevLimit = buckets[i]*64;
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
		int version = instr.readInt();
		if (cVersion != version) {
			throw new IllegalStateException("StorageStats object is wrong version");
		}
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
		
		outd.writeInt(cVersion);
		
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
		findBlobBucket(sze).allocate(sze);
	}
	
	public void deleteBlob(int sze) {
		m_blobDeletion -= sze;
		
		// decrement blob bucket
		findBlobBucket(sze).delete(sze);
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

	/**
	 * Collected statistics are against each Allocation Block size:
	 * <dl>
	 * <dt>AllocatorSize</dt><dd>The #of bytes in the allocated slots issued by this allocator.</dd>
	 * <dt>AllocatorCount</dt><dd>The #of fixed allocators for that slot size.</dd>
	 * <dt>SlotsInUse</dt><dd>The difference between the two previous columns (net slots in use for this slot size).</dd>
	 * <dt>SlotsReserved</dt><dd>The #of slots in this slot size which have had storage reserved for them.</dd>
	 * <dt>SlotsAllocated</dt><dd>Cumulative allocation of slots to date in this slot size (regardless of the transaction outcome).</dd>
	 * <dt>SlotsRecycled</dt><dd>Cumulative recycled slots to date in this slot size (regardless of the transaction outcome).</dd>
	 * <dt>SlotsChurn</dt><dd>How frequently slots of this size are re-allocated (SlotsInUse/SlotsAllocated).</dd>
	 * <dt>%SlotsUnused</dt><dd>The percentage of slots of this size which are not in use (1-(SlotsInUse/SlotsReserved)).</dd>
	 * <dt>BytesReserved</dt><dd>The space reserved on the backing file for those allocation slots</dd>
	 * <dt>BytesAppData</dt><dd>The #of bytes in the allocated slots which are used by application data (including the record checksum).</dd>
	 * <dt>%SlotWaste</dt><dd>How well the application data fits in the slots (BytesAppData/(SlotsInUse*AllocatorSize)).</dd>
	 * <dt>%AppData</dt><dd>How much of your data is stored by each allocator (BytesAppData/Sum(BytesAppData)).</dd>
	 * <dt>%StoreFile</dt><dd>How much of the backing file is reserved for each allocator (BytesReserved/Sum(BytesReserved)).</dd>
	 * <dt>%StoreWaste</dt><dd>How much of the total waste on the store is waste for this allocator size ((BytesReserved-BytesAppData)/(Sum(BytesReserved)-Sum(BytesAppData))).</dd>
	 * </dl>
	 * 
	 * @param str
	 * 
	 * FIXME Javadoc edit - this has diverged from the comments above. Also, there 
	 * is also a divideByZero which can appear (this has been fixed).<pre>
     [java] Exception in thread "main" java.lang.ArithmeticException: / by zero
     [java] 	at java.math.BigDecimal.divideAndRound(BigDecimal.java:1407)
     [java] 	at java.math.BigDecimal.divide(BigDecimal.java:1381)
     [java] 	at java.math.BigDecimal.divide(BigDecimal.java:1491)
     [java] 	at com.bigdata.rwstore.StorageStats$Bucket.slotsUnused(StorageStats.java:240)
     [java] 	at com.bigdata.rwstore.StorageStats.showStats(StorageStats.java:448)
     [java] 	at com.bigdata.rwstore.RWStore.showAllocators(RWStore.java:2620)
     [java] 	at com.bigdata.rdf.store.DataLoader.main(DataLoader.java:1415)
     </pre>
	 */
	public void showStats(StringBuilder str) {
		str.append("\n-------------------------\n");
		str.append("RWStore Allocator Summary\n");
		str.append("-------------------------\n");
		str.append(String.format("%-16s %16s %16s %16s %16s %16s %16s %16s %16s %16s %16s %16s %16s %16s %16s %16s %16s %16s \n", 
				"AllocatorSize",
				"AllocatorCount",
				"SlotsAllocated",
				"%SlotsAllocated",
				"SlotsRecycled",
				"SlotChurn",
				"SlotsInUse",
				"%SlotsInUse",
				"MeanAllocation",
				"SlotsReserved",
				"%SlotsUnused",
				"BytesReserved",
				"BytesAppData",
				"%SlotWaste",
				"%AppData",
				"%StoreFile",
				"%TotalWaste",
				"%FileWaste"
				));
		
		long totalAppData = 0;
		long totalFileStore = 0;
		long totalAllocations = 0;
		long totalInuse = 0;
		for (Bucket b: m_buckets) {
			totalAppData += b.usedStore();
			totalFileStore += b.reservedStore();
			totalAllocations += b.m_slotAllocations;
			totalInuse += b.usedSlots();
		}
		long totalWaste = totalFileStore - totalAppData;
		
		for (Bucket b: m_buckets) {
			str.append(String.format("%-16d %16d %16d %16.2f %16d %16.2f %16d %16.2f %16d %16d %16.2f %16d %16d %16.2f %16.2f %16.2f %16.2f  %16.2f \n",
				b.m_size,
				b.m_allocators,
				b.m_slotAllocations,
				b.percentAllocations(totalAllocations),
				b.m_slotDeletes,
				b.slotChurn(),
				b.usedSlots(),
				b.percentSlotsInuse(totalInuse),
				b.meanAllocation(),
				b.m_totalSlots,
				b.slotsUnused(),
				b.reservedStore(),
				b.usedStore(),
				b.slotWaste(),
				dataPercent(b.usedStore(), totalAppData),
				dataPercent(b.reservedStore(), totalFileStore),
				b.totalWaste(totalWaste),
				b.totalWaste(totalFileStore)
			));
		}
		
		str.append("\n-------------------------\n");
		str.append("BLOBS\n");
		str.append("-------------------------\n");
		str.append(String.format("%-10s %12s %12s %12s %12s %12s %12s %12s %12s\n", 
			"Bucket(K)",
			"Allocations",
			"Allocated",
			"Deletes",
			"Deleted",
			"Current",
			"Data",
			"Mean",
			"Churn"));

		for (BlobBucket b: m_blobBuckets) {
			str.append(String.format("%-10d %12d %12d %12d %12d %12d %12d %12d %12.2f\n", 
				b.m_size/1024,
				b.m_allocations,
				b.m_allocationSize,
				b.m_deletes,
				b.m_deleteSize,
				(b.m_allocations - b.m_deletes),
				(b.m_allocationSize - b.m_deleteSize),
				b.meanAllocation(),
				b.churn()
			));
		}
		
	}

	private float dataPercent(long usedData, long totalData) {
		BigDecimal used = new BigDecimal(100 * usedData);
		BigDecimal total = new BigDecimal(totalData);
		
		return used.divide(total, 2, RoundingMode.HALF_UP).floatValue();
	}
}
