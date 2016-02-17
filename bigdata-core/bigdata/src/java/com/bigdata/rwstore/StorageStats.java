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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Iterator;

import com.bigdata.rwstore.sector.SectorAllocator;

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
		// See BLZG-1646 BytesAppData counter can not be tracked accurately and should be removed
//		long m_deleteSize;
		
		// By copying committed data the stats can be reset on abort
		BlobBucket m_committed = null;
		
		public BlobBucket(final int size) {
			m_size = size;
		}
		public BlobBucket(final DataInputStream instr) throws IOException {
			m_size = instr.readInt();
			m_allocationSize = instr.readLong();
			m_allocations = instr.readLong();
			// See BLZG-1646 BytesAppData counter can not be tracked accurately and should be removed
			instr.readLong(); // was m_deleteSize
//			m_deleteSize = instr.readLong();
			m_deletes = instr.readLong();
			
			commit();
		}
		
		public void write(final DataOutputStream outstr) throws IOException {
			outstr.writeInt(m_size);
			outstr.writeLong(m_allocationSize);
			outstr.writeLong(m_allocations);
            // See BLZG-1646 BytesAppData counter can not be tracked accurately and should be removed
            outstr.writeLong(0L); // was m_deleteSize
//            outstr.writeLong(m_deleteSize);
			outstr.writeLong(m_deletes);
		}
		public void commit() {
			if (m_committed == null) {
				m_committed = new BlobBucket(m_size);
			}
			m_committed.m_allocationSize = m_allocationSize;
			m_committed.m_allocations = m_allocations;
            // See BLZG-1646 BytesAppData counter can not be tracked accurately and should be removed
//			m_committed.m_deleteSize = m_deleteSize;
			m_committed.m_deletes = m_deletes;
		}
		
		public void reset() {
			if (m_committed != null) {
				m_allocationSize = m_committed.m_allocationSize;
				m_allocations = m_committed.m_allocations;
	            // See BLZG-1646 BytesAppData counter can not be tracked accurately and should be removed
//				m_deleteSize = m_committed.m_deleteSize;
				m_deletes = m_committed.m_deletes;
			} else {
				m_allocationSize = 0;
				m_allocations = 0;
	            // See BLZG-1646 BytesAppData counter can not be tracked accurately and should be removed
//				m_deleteSize = 0;
				m_deletes = 0;
			}
		}
		
		public void delete(final int sze) {
            // See BLZG-1646 BytesAppData counter can not be tracked accurately and should be removed
//			m_deleteSize += sze;
			m_deletes++;
		}
		public void allocate(final int sze) {
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

		/*
		 * Remove because we lack the concept of reserved slots for blobs. Just
		 * look at the 8k allocators churn.
		 */
//		public float churn() {
//			
//		    if (active() == 0)
//				return m_allocations;
//			
//			final BigDecimal allocs = new BigDecimal(m_allocations);
//			
//			final BigDecimal used = new BigDecimal(active());			
//			
//			return allocs.divide(used, 2, RoundingMode.HALF_UP).floatValue();
//		}
		
	}
	
	public class Bucket {
		final int m_start;
		/** AllocatorSize: The #of bytes in the allocated slots issued by this allocator. */
		final int m_size;
		/** AllocatorCount: The #of fixed allocators for that slot size. */
		int m_allocators;
		/** SlotsReserved: The #of slots in this slot size which have had storage reserved for them. */
		long m_totalSlots;
		/** SlotsAllocated: Cumulative allocation of slots to date in this slot size (regardless of the transaction outcome). */
		long m_slotAllocations;
		/** SlotsRecycled: Cumulative recycled slots to date in this slot size (regardless of the transaction outcome). */
		long m_slotDeletes;
		/** The user bytes in use across all allocations for this slot size (does not consider recycled or deleted slots). */
		long m_sizeAllocations;
//		/**
//		 * The user bytes that were in use across all allocations for this slot
//		 * size that have been recycled / deleted.
//		 * <p>
//		 * Note: Per BLZG-1551, this value is not tracked accurately!
//		 * 
//		 * @see BLZG-1551 (Storage statistics documentation and corrections)
//         * See BLZG-1646 BytesAppData counter can not be tracked accurately and should be removed
//		 */
//		long m_sizeDeletes;
		
		// By copying committed data the stats can be reset on abort
		Bucket m_committed = null;
		
		public Bucket(final int size, final int startRange) {
			m_size = size;
			m_start = startRange;
		}

		public Bucket(final DataInputStream instr) throws IOException {
			m_size = instr.readInt();
			m_start = instr.readInt();
			m_allocators = instr.readInt();
			m_slotAllocations = instr.readLong();
			m_slotDeletes = instr.readLong();
			m_totalSlots = instr.readLong();
			m_sizeAllocations = instr.readLong();
			// See BLZG-1646 BytesAppData counter can not be tracked accurately and should be removed
            instr.readLong(); // was m_sizeDeletes
//            m_sizeDeletes = instr.readLong();
			
			commit();
		}
		
		public void write(final DataOutputStream outstr) throws IOException {
			outstr.writeInt(m_size);
			outstr.writeInt(m_start);
			outstr.writeInt(m_allocators);
			outstr.writeLong(m_slotAllocations);
			outstr.writeLong(m_slotDeletes);
			outstr.writeLong(m_totalSlots);
			outstr.writeLong(m_sizeAllocations);
            // See BLZG-1646 BytesAppData counter can not be tracked accurately and should be removed
			outstr.writeLong(0L); // was m_sizeDeletes
//			outstr.writeLong(m_sizeDeletes);
		}
		
		public void commit() {
			if (m_committed == null) {
				m_committed = new Bucket(m_size, m_start);
			}
			m_committed.m_allocators = m_allocators;
			m_committed.m_slotAllocations = m_slotAllocations;
			m_committed.m_slotDeletes = m_slotDeletes;
			m_committed.m_totalSlots = m_totalSlots;
			m_committed.m_sizeAllocations = m_sizeAllocations;
            // See BLZG-1646 BytesAppData counter can not be tracked accurately and should be removed
//			m_committed.m_sizeDeletes = m_sizeDeletes;
		}
		
		public void reset() {
			if (m_committed != null) {
				m_allocators = m_committed.m_allocators;
				m_slotAllocations = m_committed.m_slotAllocations;
				m_slotDeletes = m_committed.m_slotDeletes;
				m_totalSlots = m_committed.m_totalSlots;
				m_sizeAllocations = m_committed.m_sizeAllocations;
	            // See BLZG-1646 BytesAppData counter can not be tracked accurately and should be removed
//				m_sizeDeletes = m_committed.m_sizeDeletes;
			} else {
				m_allocators = 0;
				m_slotAllocations = 0;
				m_slotDeletes = 0;
				m_totalSlots =0;
				m_sizeAllocations = 0;
	            // See BLZG-1646 BytesAppData counter can not be tracked accurately and should be removed
//				m_sizeDeletes = 0;
			}
		}
		
		public void delete(final int sze) {
			if (sze < 0)
				throw new IllegalArgumentException("delete requires positive size, got: " + sze);
			
//			if (m_size > 64 && sze < 64) {
//				/*
//				 * If called from deferFree then may not include size. If so
//				 * then use average size of slots to date as best running
//				 * estimate.
//				 * 
//				 * @see BLZG-1551 (Storage statistics documentation and corrections)
//				 */
//				sze = meanAllocation();
//			}
			
			if (sze > m_size) {
				// sze = ((sze - 1 + m_maxFixed)/ m_maxFixed) * 4; // Blob header
				
				throw new IllegalArgumentException("Deletion of address with size greater than slot - " + sze + " > " + m_size);
			}
			
            // See BLZG-1646 BytesAppData counter can not be tracked accurately and should be removed
//			m_sizeDeletes += sze;
			m_slotDeletes++;
		}
		
		public void allocate(final int sze) {
			if (sze <= 0)
				throw new IllegalArgumentException("allocate requires positive size, got: " + sze);
			
			m_sizeAllocations += sze;
			m_slotAllocations++;
		}
		
		public void addSlots(final int slots) {
			m_totalSlots += slots;
		}
		
		/** SlotsInUse: SlotsAllocated - SlotsRecycled (net slots in use for this slot size). */
		public long usedSlots() {
			return m_slotAllocations - m_slotDeletes;
		}
		
		public long emptySlots() {
			return m_totalSlots - usedSlots();
		}
		
		/**
		 * BytesAppData: The #of bytes in the allocated slots which are used by
		 * application data (including the record checksum).
		 * 
		 * See BLZG-1551 : The data reported here used to be bad since 
		 * {@link #m_sizeDeletes} could not being tracked correctly in the
		 * {@link StorageStats} because we do not know the actual #of bytes
		 * in the slot that contain application data when we finally recycle
		 * the slot.
		 * 
		 * See BLZG-1646 BytesAppData counter can not be tracked accurately and should be removed
		 */
		public long usedStore() {
		    return usedSlots() * m_size;
//			return m_sizeAllocations - m_sizeDeletes;
		}
		
		/** %SlotWaste: How well the application data fits in the slots (BytesAppData/(SlotsInUse*AllocatorSize)). */
		public float slotWaste() {	
			if (usedStore() == 0)
				return 0.0f;
			
			final BigDecimal size = new BigDecimal(reservedStore());
			final BigDecimal store = new BigDecimal(100 * (reservedStore() - usedStore()));
			if(size.signum()==0) return 0f;
			return store.divide(size, 2, RoundingMode.HALF_UP).floatValue();
		}

		/*
		 * TODO This is invoked for both %TotalWaste and %FileWaste. Is it
		 * correctly invoked for both statistics? Note that we do not have an
		 * official definition of %TotalWaste. See BLZG-1551.
		 */
		public float totalWaste(final long total) {	

			if (total == 0)
				return 0.0f;
			
			final long slotWaste = reservedStore() - usedStore();
			
			final BigDecimal localWaste = new BigDecimal(100 * slotWaste);
			final BigDecimal totalWaste = new BigDecimal(total);			
			
			if(totalWaste.signum()==0) return 0f;
			
			return localWaste.divide(totalWaste, 2, RoundingMode.HALF_UP).floatValue();
		}
		
		/**
		 * BytesReserved: The space reserved on the backing file for those allocation slots (AllocatorSlots * SlotsReserved). */
		public long reservedStore() {
			return m_size * m_totalSlots;
		}
		
		public long reservedSlots() {
			return m_totalSlots;
		}
		
		public void addAlocator() {
			m_allocators++;
		}
		
		/**
         * SlotsChurn: A measure of how frequently slots of this size are
         * re-allocated provided by slotsAllocated/slotsReserved. This metric is
         * higher when there are more allocations made against a given #of slots
         * reserved.
         */
		public float slotChurn() {
		
            final BigDecimal slotsAllocated = new BigDecimal(m_slotAllocations);
            
			final BigDecimal slotsReserved = new BigDecimal(m_totalSlots);
			
			if (slotsReserved.signum() == 0)
				return 0f;
			
			return slotsAllocated.divide(slotsReserved, 2, RoundingMode.HALF_UP).floatValue();
			
		}
		
		/** %SlotsUnused: The percentage of slots of this size which are not in use (1-(SlotsInUse/SlotsReserved)). */
		public float slotsUnused() {
			if (m_totalSlots == 0) {
				return 0.0f;
			}
			
			BigDecimal used = new BigDecimal(100 * (m_totalSlots-usedSlots()));			
			BigDecimal total = new BigDecimal(m_totalSlots);
			if(total.signum()==0) return 0f;
			return used.divide(total, 2, RoundingMode.HALF_UP).floatValue();
		}
		
		/**
		 * %SlotsAllocated: SlotsAllocated/(Sum of SlotsAllocated across all
		 * slot sizes).
		 * 
		 * @param totalAllocations
		 *            The #of allocations across all slot sizes.
		 * @return
		 */
		public float percentAllocations(final long totalAllocations) {
			if (totalAllocations == 0) {
				return 0.0f;
			}
			
			final BigDecimal used = new BigDecimal(100 * m_slotAllocations);			
			final BigDecimal total = new BigDecimal(totalAllocations);
			if(total.signum()==0) return 0f;
			return used.divide(total, 2, RoundingMode.HALF_UP).floatValue();
		}
		
		/**
		 * %SlotsInUse: SlotsInUse / (total SlotsInUse across all slots sizes).
		 * 
		 * @param totalInuse
		 *            The total of SlotsInUse across all slot sizes.
		 */
		public float percentSlotsInuse(final long totalInuse) {
			
			if (totalInuse == 0) {
				return 0.0f;
			}
			
			final BigDecimal used = new BigDecimal(100 * usedSlots());	
			
			final BigDecimal total = new BigDecimal(totalInuse);
			
			if(total.signum()==0) return 0f;
			
			return used.divide(total, 2, RoundingMode.HALF_UP).floatValue();
		}
		
		/**
		 * MeanAllocation: (total application bytes used across all allocations for this slot size) / SlotsAllocated
		 * @return
		 */
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
	
	public Bucket findBucket(final int sze) {
		for (Bucket b : m_buckets) {
			if (sze == b.m_size)
				return b;
		}
		
		throw new IllegalStateException("Buckets have not been correctly set");
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
	 * <dt>SlotsAllocated</dt><dd>Cumulative allocation of slots to date in this slot size (regardless of the transaction outcome).</dd>
	 * <dt>%SlotsAllocated</dt><dd>SlotsAllocated/(Sum of SlotsAllocated across all slot sizes).</dd>
	 * <dt>SlotsRecycled</dt><dd>Cumulative recycled slots to date in this slot size (regardless of the transaction outcome).</dd>
	 * <dt>SlotsChurn</dt><dd>How frequently slots of this size are re-allocated (SlotsAllocated/SlotsReserved).</dd>
	 * <dt>SlotsInUse</dt><dd>SlotsAllocated - SlotsRecycled (net slots in use for this slot size).</dd>
	 * <dt>%SlotsInUse</dt><dd>SlotsInUse / (total SlotsInUse across all slots sizes).</dd>
	 * <dt>MeanAllocation</dt><dd>((Total application bytes used across all allocations for this slot size) / SlotsAllocated).</dd>
	 * <dt>SlotsReserved</dt><dd>The #of slots in this slot size which have had storage reserved for them.</dd>
	 * <dt>%SlotsUnused</dt><dd>The percentage of slots of this size which are not in use (1-(SlotsInUse/SlotsReserved)).</dd>
	 * <dt>BytesReserved</dt><dd>The space reserved on the backing file for those allocation slots (AllocatorSlots * SlotsReserved).</dd>
	 * <dt>UsedStore</dt><dd>The #of bytes in the allocated slots.</dd>
	 * <dt>%SlotWaste</dt><dd>How well the application data fits in the slots (BytesAppData/(SlotsInUse*AllocatorSize)).</dd>
	 * <dt>%AppData</dt><dd>How much of your data is stored by each allocator (BytesAppData/Sum(BytesAppData)).</dd>
	 * <dt>%StoreFile</dt><dd>How much of the backing file is reserved for each allocator (BytesReserved/Sum(BytesReserved)).</dd>
	 * TODO TotalWaste
	 * <dt>%StoreWaste</dt><dd>How much of the total waste on the store is waste for this allocator size ((BytesReserved-BytesAppData)/(Sum(BytesReserved)-Sum(BytesAppData))).</dd>
	 * </dl>
	 * 
	 * @param str The allocator statistics will be appended to the caller's buffer.
	 * 
     * @see BLZG-1646 BytesAppData counter can not be tracked accurately and should be removed
	 */
	public void showStats(final StringBuilder str) {
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
		
		// aggregate over all slot sizes
		long totalAppData = 0; // total BytesAppData across all slot sizes.
		long totalFileStore = 0; // total BytesReserved across all slot sizes.
		long totalAllocations = 0; // total SlotsAllocated across all slot sizes.
		long totalInuse = 0; // total SlotsInUse across all slots sizes.
		for (Bucket b: m_buckets) {
			totalAppData += b.usedStore();
			totalFileStore += b.reservedStore();
			totalAllocations += b.m_slotAllocations;
			totalInuse += b.usedSlots();
		}
		final long totalWaste = totalFileStore - totalAppData;
		
		for (Bucket b: m_buckets) {
			str.append(String.format("%-16d %16d %16d %16.2f %16d %16.2f %16d %16.2f %16d %16d %16.2f %16d %16d %16.2f %16.2f %16.2f %16.2f  %16.2f \n",
				b.m_size, // AllocatorSize
				b.m_allocators, // AllocatorCount
				b.m_slotAllocations, // SlotsAllocated
				b.percentAllocations(totalAllocations), // %SlotsAllocated 
				b.m_slotDeletes, // SlotsRecycled
				b.slotChurn(), // SlotChurn
				b.usedSlots(), // SlotsInUse
				b.percentSlotsInuse(totalInuse), // %SlotsInUse
				b.meanAllocation(), // MeanAllocation
				b.m_totalSlots, // SlotsReserved
				b.slotsUnused(), // %SlotsUnused
				b.reservedStore(), // BytesReserved
				b.usedStore(), // UsedStore 
				b.slotWaste(), // %SlotWaste
				dataPercent(b.usedStore(), totalAppData), // %AppData
				dataPercent(b.reservedStore(), totalFileStore), // %StoreFile
				b.totalWaste(totalWaste), // %TotalWaste
				b.totalWaste(totalFileStore) // %FileWaste
			));
		}
		
		str.append("\n-------------------------\n");
		str.append("BLOBS\n");
		str.append("-------------------------\n");
		str.append(String.format("%-10s %12s %12s %12s %12s %12s\n",// %12s\n", 
			"Bucket(K)",
			"Allocations",
			"Allocated",
			"Deletes",
//			"Deleted", // 4
			"Current",
//			"Data", // 6
			"Mean"
//			"Churn"
			));

		for (BlobBucket b: m_blobBuckets) {
			str.append(String.format("%-10d %12d %12d %12d %12d %12d\n",// %12.2f\n", 
				b.m_size/1024, // Bucket(K)
				b.m_allocations, // Allocations
				b.m_allocationSize, // Allocated
				b.m_deletes, // Deletes
//				b.m_deleteSize, // Deleted
				(b.m_allocations - b.m_deletes), // Current
//				(b.m_allocationSize - b.m_deleteSize), // Data
				b.meanAllocation() // Mean
//				b.churn()
			));
		}
		
	}

	/**
	 * Helper method returns the ratio <code>(usedData/totalData)</code> as a
	 * percentage.
	 * 
	 * @param usedData
	 * @param totalData
	 * @return
	 */
	static private float dataPercent(final long usedData, final long totalData) {

		if (totalData == 0)
			return 0.0f;
		
		final BigDecimal used = new BigDecimal(100 * usedData);
		final BigDecimal total = new BigDecimal(totalData);
		
		return used.divide(total, 2, RoundingMode.HALF_UP).floatValue();

	}

	public void register(SectorAllocator allocator, boolean init) {
		throw new UnsupportedOperationException();
	}

	public void commit() {
		for (Bucket b: m_buckets) {
			b.commit();
		}
		for (BlobBucket b: m_blobBuckets) {
			b.commit();
		}
	}
	public void reset() {
		for (Bucket b: m_buckets) {
			b.reset();
		}
		for (BlobBucket b: m_blobBuckets) {
			b.reset();
		}
	}
	
	public Iterator<Bucket> getBuckets() {
		return m_buckets.iterator();
	}
}
