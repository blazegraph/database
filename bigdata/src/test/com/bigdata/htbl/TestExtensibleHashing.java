/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
/*
 * Created on Nov 22, 2010
 */
package com.bigdata.htbl;

import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;

import junit.framework.TestCase2;

/**
 * Test suite for extensible hashing.
 * 
 * @todo Persistence capable hash table for high volume hash joins. The data
 *       will be "rows" in a "relation" modeled using binding sets. We can use
 *       dense encoding of these rows since they have a fixed schema (some
 *       columns may allow nulls). There should also be a relationship to how we
 *       encode these data for network IO.
 * 
 * @todo Extensible hashing:
 *       <p>
 *       - hash(byte[] key) -> IRaba page. Use IRaba for keys/values and key
 *       search.
 *       <p>
 *       - Split if overflows the bucket size (alternative is some versioning
 *       where the computed hash value indexes into a logical address which is
 *       then translated to an IRawStore address - does the RWStore help us out
 *       here?)
 *       <p>
 *       - Ring buffer to wire in hot nodes (but expect random touches).
 *       <p>
 *       - initially, no history (no versioning). just replace the record when
 *       it is evicted from the ring buffer.
 *       <p>
 *       What follows is a summary of an extensible hashing design for bigdata.
 *       This covers most aspects of the hash map design, but does not drill
 *       deeply into the question of scale-out hash maps. The immediate goal is
 *       to develop a hash map which can be used for a variety of tasks,
 *       primarily pertaining to analytic query as described above.
 *       <p>
 *       Extensible hashing is one form of dynamic hashing in which buckets are
 *       split or coalesced as necessary and in which the reorganization is
 *       performed on one bucket at a time.
 *       <p>
 *       Given a hash function h generating, e.g., int32 values where b is the
 *       #of bits in the hash code. At any point, we use 0 LTE i LTE b bits of
 *       that hash code as an index into a table of bucket addresses. The value
 *       of i will change as the #of buckets changes based on the scale of the
 *       data to be addressed.
 *       <p>
 *       Given a key K, the bucket address table is indexed with i bits of the
 *       hash code, h(K). The value at that index is the address of the hash
 *       bucket. However, several consecutive entries in the hash table may
 *       point to the same hash bucket (for example, the hash index may be
 *       created with i=4, which would give 16 index values but only one initial
 *       bucket). The bucket address table entries which map onto the same hash
 *       bucket will have a common bit length, which may be LTE [i]. This bit
 *       length is not stored in the bucket address table, but each bucket knows
 *       its bit length. Given a global bit length of [i] and a bucket bit
 *       length of [j], there will be 2^(i-j) bucket address table entries which
 *       point to the same bucket.
 *       <p>
 *       Hash table versioning can be easily implemented by: (a) a checkpoint
 *       record with the address of the bucket address table (which could be
 *       broken into a two level table comprised of 4k pages in order to make
 *       small updates faster); and (b) a store level policy such that we do not
 *       overwrite the modified records directly (though they may be recycled).
 *       This will give us the same consistent read behind behavior as the
 *       B+Tree.
 *       <p>
 *       The IIndex interface will need to be partitioned appropriately such
 *       that the IRangeScan interface is not part of the hash table indices (an
 *       isBTree() and isHashMap() method might be added).
 *       <p>
 *       While the same read-through views for shards should work with hash maps
 *       as work with B+Tree indices, a different scheme may be necessary to
 *       locate those shards and we might need to use int64 hash codes in
 *       scale-out or increase the page size (at least for the read-only hash
 *       segment files, which would also need a batch build operation). The
 *       AccessPath will also need to be updated to be aware of classes which do
 *       not support key-range scans, but only whole relation scans.
 *       <p>
 *       Locking on hash tables without versioning should be much simpler than
 *       locking on B+Trees since there is no hierarchy and more operations can
 *       proceed without blocking in parallel.
 *       <p>
 *       We can represent tuples (key,value pairs) in an IRaba data structure
 *       and reuse parts of the B+Tree infrastructure relating to compression of
 *       IRaba, key search, etc. In fact, we might use to lazy reordering notion
 *       from Monet DB cracking to only sort the keys in a bucket when it is
 *       persisted. This is also a good opportunity to tackling splitting the
 *       bucket if it overflows the target record size, e.g., 4k. We could throw
 *       out an exception if the sorted, serialized, and optionally compressed
 *       record exceeds the target record size and then split the bucket. All of
 *       this seems reasonable and we might be able to then back port those
 *       concepts into the B+Tree.
 *       <p>
 *       We need to estimate the #of tuples which will fit within the bucket. We
 *       can do this based on: (a) the byte length of the keys and values (key
 *       compression is not going to help out much for a hash index since the
 *       keys will be evenly distributed even if they are ordered within a
 *       bucket); (b) the known per tuple overhead and per bucket overhead; (c)
 *       an estimate of the compression ratio for raba encoding and record
 *       compression. This estimate could be used to proactively split a bucket
 *       before it is evicted. This is most critical before anything is evicted
 *       as we would otherwise have a single very large bucket. So, let's make
 *       this simple and split the bucket if the sum of the key + val bytes
 *       exceeds 120% of the target record size (4k, 8k, etc). The target page
 *       size can be a property of the hash index. [Note: There is an implicit
 *       limit on the size of a tuple with this approach. The alternative is to
 *       fix the #of tuples in the bucket and allow buckets to be of whatever
 *       size they are for the specific data in that bucket.]
 * 
 * @todo RWStore integration notes:
 *       <p>
 *       - RWStore with "temporary" quality. Creates the backing file lazily on
 *       eviction from the write service.
 *       <p>
 *       - RWStore with "RAM" only? (Can not exceed the #of allocated buffers or
 *       can, but then it might force paging out to swap?)
 *       <p>
 *       - RWStore with "RAM" mostly. Converts to disk backed if uses all those
 *       buffers. Possibly just give the WriteCacheService a bunch of write
 *       cache buffers (10-100) and have it evict to disk *lazily* rather than
 *       eagerly (when the #of free buffers is down to 20%).
 *       <p>
 *       - RWStore with memory mapped file? As I recall, the problem is that we
 *       can not guarantee extension or close of the file under Java. But some
 *       people seem to make this work...
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/203
 */
public class TestExtensibleHashing extends TestCase2 {

	public TestExtensibleHashing() {
	}

	public TestExtensibleHashing(String name) {
		super(name);
	}

	/**
	 * Unit test for {@link SimpleExtensibleHashMap#getMapSize(int)}.
	 */
	public void test_getMapSize() {
		
		assertEquals(1/* addressSpaceSize */, SimpleExtensibleHashMap.getMapSize(1)/* initialCapacity */);
		assertEquals(1/* addressSpaceSize */, SimpleExtensibleHashMap.getMapSize(2)/* initialCapacity */);
		assertEquals(2/* addressSpaceSize */, SimpleExtensibleHashMap.getMapSize(3)/* initialCapacity */);
		assertEquals(2/* addressSpaceSize */, SimpleExtensibleHashMap.getMapSize(4)/* initialCapacity */);
		assertEquals(3/* addressSpaceSize */, SimpleExtensibleHashMap.getMapSize(5)/* initialCapacity */);
		assertEquals(3/* addressSpaceSize */, SimpleExtensibleHashMap.getMapSize(6)/* initialCapacity */);
		assertEquals(3/* addressSpaceSize */, SimpleExtensibleHashMap.getMapSize(7)/* initialCapacity */);
		assertEquals(3/* addressSpaceSize */, SimpleExtensibleHashMap.getMapSize(8)/* initialCapacity */);
		assertEquals(4/* addressSpaceSize */, SimpleExtensibleHashMap.getMapSize(9)/* initialCapacity */);

		assertEquals(5/* addressSpaceSize */, SimpleExtensibleHashMap.getMapSize(32)/* initialCapacity */);

		assertEquals(10/* addressSpaceSize */, SimpleExtensibleHashMap.getMapSize(1024)/* initialCapacity */);

	}

	/**
	 * Unit test for {@link SimpleExtensibleHashMap#getMaskBits(int)}
	 */
	public void test_getMaskBits() {

		assertEquals(0x00000001, SimpleExtensibleHashMap.getMaskBits(1));
		assertEquals(0x00000003, SimpleExtensibleHashMap.getMaskBits(2));
		assertEquals(0x00000007, SimpleExtensibleHashMap.getMaskBits(3));
		assertEquals(0x0000000f, SimpleExtensibleHashMap.getMaskBits(4));
		assertEquals(0x0000001f, SimpleExtensibleHashMap.getMaskBits(5));
		assertEquals(0x0000003f, SimpleExtensibleHashMap.getMaskBits(6));
		assertEquals(0x0000007f, SimpleExtensibleHashMap.getMaskBits(7));
		assertEquals(0x000000ff, SimpleExtensibleHashMap.getMaskBits(8));

		assertEquals(0x0000ffff, SimpleExtensibleHashMap.getMaskBits(16));

		assertEquals(0xffffffff, SimpleExtensibleHashMap.getMaskBits(32));
		
	}

	/**
	 * Unit test for {@link SimpleExtensibleHashMap#maskOff(int, int)}
	 */
	public void test_maskOff() {

//		SimpleExtensibleHashMap.class;
		
		assertEquals(0x00000000, SimpleExtensibleHashMap
				.maskOff(0/* hash */, 1/* nbits */));

		assertEquals(0x00000000, SimpleExtensibleHashMap
				.maskOff(8/* hash */, 2/* nbits */));

		assertEquals(0x00000002, SimpleExtensibleHashMap
				.maskOff(18/* hash */, 2/* nbits */));

	}
	
	/**
	 * Extensible hashing data structure.
	 * 
	 * @todo allow duplicate tuples - caller can enforce distinct if they like.
	 * 
	 * @todo automatically promote large tuples into raw record references,
	 *       leaving the hash code of the key and the address of the raw record
	 *       in the hash bucket.
	 * 
	 * @todo initially manage the address table in an int[].
	 * 
	 * @todo use 4k buckets. split buckets when the sum of the data is GT 4k
	 *       (reserve space for a 4byte checksum). use a compact record
	 *       organization. if a tuple is deleted, bit flag it (but immediately
	 *       delete the raw record if one is associated with the tuple). before
	 *       splitting, compact the bucket to remove any deleted tuples.
	 * 
	 * @todo the tuple / raw record promotion logic should be shared with the
	 *       B+Tree. The only catch is that large B+Tree keys will always remain
	 *       a stress factor. For example, TERM2ID will have large B+Tree keys
	 *       if TERM is large and promoting to a blob will not help. In that
	 *       case, we actually need to hash the TERM and store the hash as the
	 *       key (or index only the first N bytes of the term).
	 */
	public static class ExtensibleHashBag {
		
	}

	/**
	 * Map constructor tests.
	 */
	public void test_ctor() {

		final SimpleExtensibleHashMap map = new SimpleExtensibleHashMap(
				1/* initialCapacity */, 3/* bucketSize */);
	
		assertEquals("globalHashBits", 1, map.getGlobalHashBits());
		
		assertEquals("addressSpaceSize", 2, map.getAddressSpaceSize());

		assertEquals("bucketCount", 1, map.getBucketCount());
		
		assertEquals("bucketSize", 3, map.getBucketSize());
		
	}
	
	/**
	 * Simple CRUD test operating against the initial bucket without triggering
	 * any splits.
	 */
	public void test_crud1() {
	
		final SimpleExtensibleHashMap map = new SimpleExtensibleHashMap(
				1/* initialCapacity */, 3/* bucketSize */);

		// a bunch of things which are not in the map.
		for (int i : new int[] { 0, 1, -4, 31, -93, 912 }) {
			
			assertFalse(map.contains(i));
			
		}
		
		/*
		 * Insert a record, then delete it, verifying that contains() reports
		 * true or false as appropriate for the pre-/post- conditions.
		 */

		assertFalse(map.contains(83));

		map.insert(83);
		
		assertTrue(map.contains(83));
		
		map.delete(83);
		
		assertFalse(map.contains(83));
		
	}

	/**
	 * CRUD test which inserts some duplicate tuples, but not enough to split
	 * the initial bucket, and the deletes them out again.
	 */
	public void test_crud2() {
	
		final SimpleExtensibleHashMap map = new SimpleExtensibleHashMap(
				1/* initialCapacity */, 3/* bucketSize */);

		assertEquals("bucketCount", 1, map.getBucketCount());
		
		assertFalse(map.contains(83));

		// insert once.
		map.insert(83);
		
		assertTrue(map.contains(83));
		
		// insert again.
		map.insert(83);
		
		assertTrue(map.contains(83));
		
		// did not split the bucket.
		assertEquals("bucketCount", 1, map.getBucketCount());

		// delete once.
		map.delete(83);

		// still found.
		assertTrue(map.contains(83));

		// delete again.
		map.delete(83);

		// now gone.
		assertFalse(map.contains(83));
		
	}

	/**
	 * Return a formatted table providing the decimal, hex, and binary
	 * representations of the given keys. The table will be displayed in
	 * ascending key order.
	 * 
	 * @param keys
	 *            The keys.
	 *            
	 * @return The table.
	 */
	private static String toString(int[] keys) {

		Arrays.sort(keys = keys.clone());

		final StringBuilder sb = new StringBuilder();

		final Formatter f = new Formatter(sb);

		sb.append("dec  hex   binary");

		for (int key : keys) {

			f.format("\n %2d   %2s %8s", key, Integer.toHexString(key), Integer
					.toBinaryString(key));

		}

		return sb.toString();
	}

	/**
	 * Test repeated insert of a key until the bucket splits.
	 * 
	 * @todo break this into two tests. One with distinct keys which cause the
	 *       split of the 1st bucket. The other a test of overflow handling when
	 *       all keys in the bucket are the same.
	 */
	public void test_split() {

		final int bucketSize = 3;
		
		final SimpleExtensibleHashMap map = new SimpleExtensibleHashMap(
				1/* initialCapacity */, bucketSize);

		assertEquals("bucketCount", 1, map.getBucketCount());

		map.insert(83);
		map.insert(83);
		map.insert(83);

		// still not split.
		assertEquals("bucketCount", 1, map.getBucketCount());

		// force a split (83 == 0b10000011)
		map.insert(83);

		assertEquals("bucketCount", 2, map.getBucketCount());

	}

	/**
	 * This unit test is patterned after an example using integer keys in
	 * "External Memory Algorithms and Data Structures" by Vitter, page 239
	 * (which is patterned after "Dynamic Hashing Schemes" by Enbody and Du,
	 * 1988). However, simply inserting the initial sequence of keys DOES NOT
	 * result in a hashing with globalHashBits := 3, per the example. Further,
	 * The example given by Vitter is simply incorrect in its initial conditions
	 * since the 3-bit hash code for <code>10</code> and <code>18</code> is
	 * <code>010</code> which would place them into the same bucket with a 3-bit
	 * address space. However, Vitter has these keys in different buckets, which
	 * is inconsistent. Therefore this unit test must be read on its own. The
	 * keys and the addressing slots may be interpreted with reference to the
	 * following tables, both of which are automatically generated when the test
	 * runs.
	 * <p>
	 * This table helps you to map from the key to the binary representation of
	 * that key. The table shows all keys used in this example.
	 * 
	 * <pre>
	 * dec  hex   binary
	 *   4    4      100
	 *   9    9     1001
	 *  10    a     1010
	 *  18   12    10010
	 *  20   14    10100
	 *  23   17    10111
	 *  32   20   100000
	 *  44   2c   101100
	 *  76   4c  1001100
	 * </pre>
	 * <p>
	 * This table helps you to map from the binary representation of the hash
	 * values back to the address table. You should consider only the lower
	 * [globalHashBits] of the hash value when looking up the decimal index into
	 * the address table.
	 * 
	 * <pre>
	 * dec  hex   binary
	 *   0    0        0
	 *   1    1        1
	 *   2    2       10
	 *   3    3       11
	 *   4    4      100
	 *   5    5      101
	 *   6    6      110
	 *   7    7      111
	 *   8    8     1000
	 *   9    9     1001
	 *  10    A     1010
	 *  11    B     1011
	 *  12    C     1100
	 *  13    D     1101
	 *  14    E     1110
	 *  15    F     1111
	 * </pre>
	 * <p>
	 * To setup the example, we insert the sequence {4, 23, 18, 10, 44, 32, 9}
	 * into an extensible hashing algorithm using buckets with a capacity of
	 * <code>3</code>. The keys and the values stored in the buckets are int32
	 * values. Inserting this sequence of keys yield a hash table with a global
	 * depth <em>d</em> of <code>2</code> having FOUR (4) addresses and THREE
	 * (3) buckets arranged as follows, where <em>k</em> is the local depth of a
	 * given bucket and the buckets are labeled A, B, and C for convenience of
	 * cross-reference.
	 * 
	 * <pre>
	 * [00] -> (A) [k=2] {4, 44, 32}
	 * [01] -> (C) [k=1] {23, 9}
	 * [10] -> (B) [k=2] {18, 10}
	 * [11] -> (C)
	 * </pre>
	 * 
	 * <p>
	 * Next, key <code>76</code> is inserted. Considering only the d=2 bits of
	 * its hash code, this key would be inserted the address having bits
	 * <code>00</code>, which is mapped onto bucket (A). Since (A) is full, this
	 * would cause bucket (A) to be split. Since there are d=2 global bits and
	 * k=2 local bits for (A), there will be 2^(2-2) == 1 entries in the address
	 * table for (A). Therefore the address space must be doubled before (A) may
	 * be split. Once the address space has been doubled, (A) is split into (A)
	 * and (D). The local depth of (A) is increased by one to <code>k=3</code>.
	 * The local depth of (D) is the same as (A). The postcondition for the hash
	 * table is as follows:
	 * 
	 * <pre>
	 * [000] -> (A) [k=3] {32}
	 * [001] -> (C) [k=1] {23, 9}
	 * [010] -> (B) [k=2] {18, 10}
	 * [011] -> (C)
	 * [100] -> (D) [k=3] {4, 44, 76}
	 * [101] -> (C)
	 * [110] -> (B)
	 * [111] -> (C)
	 * </pre>
	 * 
	 * Finally, key <code>20</code> is inserted, again into the block addressed
	 * by address table entry <code>100</code>, which is (D). Since (D) is full,
	 * this will cause it to be split. Since there are d=3 global bits and k=3
	 * local bits for (D), there will be 2^(3-3) == 1 entries in the address
	 * table for (D). Therefore the address space must be doubled before (D) may
	 * be split. Once the address space has been doubled, (D) is split into two
	 * blocks (D) and (E) having local bits <code>k=4</code>. The hash table
	 * after this expansion looks as follows:
	 * 
	 * <pre>
	 * [0000] -> (A) [k=3] {32}
	 * [0001] -> (C) [k=1] {23, 9}
	 * [0010] -> (B) [k=2] {18, 10}
	 * [0011] -> (C)
	 * [0100] -> (D) [k=4] {4, 20}
	 * [0101] -> (C)
	 * [0110] -> (B)
	 * [0111] -> (C)
	 *  ---- extension ----
	 * [1000] -> (A)
	 * [1001] -> (C)
	 * [1010] -> (B)
	 * [1011] -> (C)
	 * [1100] -> (E) [k=4] {44, 76}
	 * [1101] -> (C)
	 * [1110] -> (B)
	 * [1111] -> (C)
	 * </pre>
	 * 
	 * When the address space is extended, the original address table entries
	 * remain at their given offsets into the new table. The new address table
	 * entries are initialized from the same entry which would have been
	 * addressed if we considered one less bit. For example, any hash code
	 * ending in <code>000</code> used to index into the first entry in the
	 * address table. After the address table is split, one more bit will be
	 * considered in the hash code of the key. Therefore, the same key will
	 * either be in the address table entry <code>0000</code> or the entry
	 * <code>1000</code>. To keep everything consistent, the address table entry
	 * for <code>1000</code> is therefore initialized from the address table
	 * entry for <code>0000</code>. In practice, all you are doing is writing a
	 * second copy of the original address table entries starting immediately
	 * after the original address table entries.
	 * <p>
	 * Personally, I find this representation much easier to interpret. You can
	 * see that the address table was simply duplicated and (E) was split into
	 * two buckets. One remains (E) and continues to be addressed from the 1st
	 * half of the address table. The other is (F) and is the only change in the
	 * 2nd half of the address table.
	 * 
	 * @todo Work in deletes also or run another test which starts at the same
	 *       point and the deletes out keys. However, when deleting keys from a
	 *       hash table the implementation has more choices about when (and
	 *       whether) to reduce the address space.
	 */
	public void test_simple() {

		/*
		 * Print out the decimal, hex, and binary representations of the keys
		 * used in this example.
		 */
        {
        	final int[] keys = new int[]{
        			// setup
        			4, 23, 18, 10, 44, 32, 9,
        			// and then
        			76, 20
        			};
        	
			// just the keys used in the example.
			System.out.println(toString(keys));

			// all values in [0:15] decimal.
			System.out.println("\n"
					+ toString(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
							11, 12, 13, 14, 15 }) + "\n");

        }
        
		final int bucketSize = 3;
        
        final SimpleExtensibleHashMap map = new SimpleExtensibleHashMap(
                1/* initialCapacity */, bucketSize);

		/*
		 * Verify the initial conditions.
		 */
		{
			assertEquals("globalHashBits", 1, map.getGlobalHashBits());
			assertEquals("addressSpace", 2, map.getAddressSpaceSize());
			assertEquals("bucketCount", 1, map.getBucketCount());
			assertSameIteratorAnyOrder(new Integer[] { }, map
					.getEntries());
			final SimpleBucket b = map.getBucketFromEntryIndex(0);
			assertEquals("localHashBits", 0, b.localHashBits);
			assertSameIteratorAnyOrder(new Integer[] { }, b.getEntries());
		}

		/*
		 * Insert the keys in sequence until the initial bucket is full.
		 */
		{

			{
				assertFalse(map.contains(4));
				map.insert(4);
				System.out.println(map.toString());
				assertEquals("globalHashBits", 1, map.getGlobalHashBits());
				assertEquals("addressSpace", 2, map.getAddressSpaceSize());
				assertEquals("bucketCount", 1, map.getBucketCount());
				assertSameIteratorAnyOrder(new Integer[] { 4 }, map
						.getEntries());
				final SimpleBucket b = map.getBucketFromEntryIndex(0);
				assertEquals("localHashBits", 0, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 4 }, b.getEntries());
			}
			{
				assertFalse(map.contains(23));
				map.insert(23);
				System.out.println(map.toString());
				assertEquals("globalHashBits", 1, map.getGlobalHashBits());
				assertEquals("addressSpace", 2, map.getAddressSpaceSize());
				assertEquals("bucketCount", 1, map.getBucketCount());
				assertSameIteratorAnyOrder(new Integer[] { 4, 23 }, map
						.getEntries());
				final SimpleBucket b = map.getBucketFromEntryIndex(0);
				assertEquals("localHashBits", 0, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 4, 23 }, b
						.getEntries());
			}
			{
				assertFalse(map.contains(18));
				map.insert(18);
				System.out.println(map.toString());
				assertEquals("globalHashBits", 1, map.getGlobalHashBits());
				assertEquals("addressSpace", 2, map.getAddressSpaceSize());
				assertEquals("bucketCount", 1, map.getBucketCount());
				assertSameIteratorAnyOrder(new Integer[] { 4, 23, 18 }, map
						.getEntries());
				// addressMap[0] => bucket 0
				final SimpleBucket b = map.getBucketFromEntryIndex(0);
				assertEquals("localHashBits", 0, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 4, 23, 18 }, b
						.getEntries());
			}

		}

		/*
		 * Insert 10 to split the first bucket. Since there are two entries in
		 * the address table for that bucket, the bucket is split but the
		 * address space is not doubled.
		 */
		{
			{
				assertFalse(map.contains(10));
				map.insert(10);
				System.out.println(map.toString());
				assertEquals("globalHashBits", 1, map.getGlobalHashBits());
				assertEquals("addressSpace", 2, map.getAddressSpaceSize());
				assertEquals("bucketCount", 2, map.getBucketCount());
				assertSameIteratorAnyOrder(new Integer[] { 4, 23, 18, 10 }, map
						.getEntries());
				// addressMap[0] => bucket 0
				SimpleBucket b = map.getBucketFromEntryIndex(0);
				assertEquals("localHashBits", 1, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 4, 10, 18 }, b
						.getEntries());
				// addressMap[0] => bucket 1
				b = map.getBucketFromEntryIndex(1);
				assertEquals("localHashBits", 1, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 23 }, b.getEntries());
			}
		}

		/*
		 * Insert 44 to split the first bucket again. Since there is now only
		 * one entry in the address table for that bucket, the address space is
		 * doubled before the bucket is split.
		 */
        {
			{
				assertFalse(map.contains(44));
				map.insert(44);
				System.out.println(map.toString());
				assertEquals("globalHashBits", 2, map.getGlobalHashBits());
				assertEquals("addressSpace", 4, map.getAddressSpaceSize());
				assertEquals("bucketCount", 3, map.getBucketCount());
				assertSameIteratorAnyOrder(new Integer[] { 4, 23, 18, 10, 44 },
						map.getEntries());
				// addressMap[0] => bucket 0.
				SimpleBucket b = map.getBucketFromEntryIndex(0);
				assertEquals("localHashBits", 2, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 4, 44 }, b
						.getEntries());
				// addressMap[1,3] => bucket 1
				assertEquals("addr", map.getAddressFromEntryIndex(1), map
						.getAddressFromEntryIndex(3));
				b = map.getBucketFromEntryIndex(1);
				assertEquals("localHashBits", 1, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 23 }, b.getEntries());
				// addressMap[2] => bucket 2
				b = map.getBucketFromEntryIndex(2);
				assertEquals("localHashBits", 2, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 10, 18 }, b.getEntries());
			}
		}

		/*
		 * Insert 32. This goes into the bucket indexed by addressMap[0]. That bucket
         * has sufficient space available so nothing is split.
		 */
        {
			{
				assertFalse(map.contains(32));
				map.insert(32);
				System.out.println(map.toString());
				assertEquals("globalHashBits", 2, map.getGlobalHashBits());
				assertEquals("addressSpace", 4, map.getAddressSpaceSize());
				assertEquals("bucketCount", 3, map.getBucketCount());
				assertSameIteratorAnyOrder(new Integer[] { 4, 23, 18, 10, 44,
						32 }, map.getEntries());
				// addressMap[0] => bucket 0.
				SimpleBucket b = map.getBucketFromEntryIndex(0);
				assertEquals("localHashBits", 2, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 4, 44, 32 }, b
						.getEntries());
				// addressMap[1,3] => bucket 1
				assertEquals("addr", map.getAddressFromEntryIndex(1), map
						.getAddressFromEntryIndex(3));
				b = map.getBucketFromEntryIndex(1);
				assertEquals("localHashBits", 1, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 23 }, b.getEntries());
				// addressMap[2] => bucket 2
				b = map.getBucketFromEntryIndex(2);
				assertEquals("localHashBits", 2, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 10, 18 }, b.getEntries());
			}
		}

        /*
         * Insert 9.  This goes into the bucket indexed by addressMap[1]. That bucket
         * has sufficient space available so nothing is split.
         */
        {
			{
				assertFalse(map.contains(9));
				map.insert(9);
				System.out.println(map.toString());
				assertEquals("globalHashBits", 2, map.getGlobalHashBits());
				assertEquals("addressSpace", 4, map.getAddressSpaceSize());
				assertEquals("bucketCount", 3, map.getBucketCount());
				assertSameIteratorAnyOrder(new Integer[] { 4, 23, 18, 10, 44,
						32, 9 }, map.getEntries());
				// addressMap[0] => bucket 0.
				SimpleBucket b = map.getBucketFromEntryIndex(0);
				assertEquals("localHashBits", 2, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 4, 44, 32 }, b
						.getEntries());
				// addressMap[1,3] => bucket 1
				assertEquals("addr", map.getAddressFromEntryIndex(1), map
						.getAddressFromEntryIndex(3));
				b = map.getBucketFromEntryIndex(1);
				assertEquals("localHashBits", 1, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 23, 9 }, b.getEntries());
				// addressMap[2] => bucket 2
				b = map.getBucketFromEntryIndex(2);
				assertEquals("localHashBits", 2, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 10, 18 }, b.getEntries());
			}
		}

		/*
		 * Note: This code works (it successfully splits the desired bucket),
		 * but it does not bring the problem back into alignment with Vitter
		 * since forcing the split of the bucket with {10,18} does not cause
		 * either of those keys to be moved into the new bucket -- they have the
		 * same 3-bit hash code. The example given by Vitter is simply incorrect
		 * in its initial conditions.
		 */
//		/*
//		 * Force the doubling of the address space and the split of the bucket
//		 * at index [2] in the address table in order to bring the state of the
//		 * hash table into alignment with the example in Vitter.
//		 */
//        {
//			{
//				map.doubleAddressSpaceAndSplitBucket(10, map
//						.getBucketFromEntryIndex(2));
//				System.out.println(map.toString());
//				assertEquals("globalHashBits", 3, map.getGlobalHashBits());
//				assertEquals("addressSpace", 8, map.getAddressSpaceSize());
//				assertEquals("bucketCount", 4, map.getBucketCount());
//				assertSameIteratorAnyOrder(new Integer[] { 4, 23, 18, 10, 44,
//						32, 9 }, map.getEntries());
//				// addressMap[0,4] => bucket 0.
//				assertEquals("addr", map.getAddressFromEntryIndex(0), map
//						.getAddressFromEntryIndex(4));
//				SimpleBucket b = map.getBucketFromEntryIndex(0);
//				assertSameIteratorAnyOrder(new Integer[] { 4, 44, 32 }, b
//						.getEntries());
//				// addressMap[1,3,5,7] => bucket 1
//				assertEquals("addr", map.getAddressFromEntryIndex(1), map
//						.getAddressFromEntryIndex(3));
//				assertEquals("addr", map.getAddressFromEntryIndex(1), map
//						.getAddressFromEntryIndex(5));
//				assertEquals("addr", map.getAddressFromEntryIndex(1), map
//						.getAddressFromEntryIndex(7));
//				b = map.getBucketFromEntryIndex(1);
//				assertSameIteratorAnyOrder(new Integer[] { 23, 9 }, b
//						.getEntries());
//				// addressMap[2] => bucket 2
//				b = map.getBucketFromEntryIndex(2);
//				assertSameIteratorAnyOrder(new Integer[] { 10, 18 }, b
//						.getEntries());
//				// addressMap[6] => bucket 3
//				b = map.getBucketFromEntryIndex(6);
//				assertSameIteratorAnyOrder(new Integer[] { }, b
//						.getEntries());
//			}
//		}
//        
//		assertEquals("globalHashBits", 3, map.getGlobalHashBits());
//		assertEquals("addressSpace", 8, map.getAddressSpaceSize());
//		assertEquals("bucketCount", 4, map.getBucketCount());

		/**
		 * Insert key 76.
		 * 
		 * Considering only the d=2 bits of its hash code, this key would be
		 * inserted the address having bits <code>00</code>, which is mapped
		 * onto bucket (A). Since (A) is full, this would cause bucket (A) to be
		 * split. Since there are d=2 global bits and k=2 local bits for (A),
		 * there will be 2^(2-2) == 1 entries in the address table for (A).
		 * Therefore the address space must be doubled before (A) may be split.
		 * Once the address space has been doubled, (A) is split into (A) and
		 * (D). The local depth of (A) is increased by one to <code>k=3</code>.
		 * The local depth of (D) is the same as (A). The postcondition for the
		 * hash table is as follows:
		 * 
		 * <pre>
		 * [000] -> (A) [k=3] {32}
		 * [001] -> (C) [k=1] {23, 9}
		 * [010] -> (B) [k=2] {18, 10}
		 * [011] -> (C)
		 * [100] -> (D) [k=3] {4, 44, 76}
		 * [101] -> (C)
		 * [110] -> (B)
		 * [111] -> (C)
		 * </pre>
		 */
        {
			{
				assertFalse(map.contains(76));
				map.insert(76);
				System.out.println(map.toString());
				assertEquals("globalHashBits", 3, map.getGlobalHashBits());
				assertEquals("addressSpace", 8, map.getAddressSpaceSize());
				assertEquals("bucketCount", 4, map.getBucketCount());
				assertSameIteratorAnyOrder(new Integer[] { 4, 23, 18, 10, 44,
						32, 9, 76 }, map.getEntries());
				// addressMap[0] => bucket 0.
				SimpleBucket b = map.getBucketFromEntryIndex(0);
				assertEquals("localHashBits", 3, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 32 }, b
						.getEntries());
				// addressMap[1,3,5,7] => bucket 1
				assertEquals("addr", map.getAddressFromEntryIndex(1), map
						.getAddressFromEntryIndex(3));
				assertEquals("addr", map.getAddressFromEntryIndex(1), map
						.getAddressFromEntryIndex(5));
				assertEquals("addr", map.getAddressFromEntryIndex(1), map
						.getAddressFromEntryIndex(7));
				b = map.getBucketFromEntryIndex(1);
				assertEquals("localHashBits", 1, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 23, 9 }, b.getEntries());
				// addressMap[2,6] => bucket 2
				b = map.getBucketFromEntryIndex(2);
				assertEquals("addr", map.getAddressFromEntryIndex(2), map
						.getAddressFromEntryIndex(6));
				assertEquals("localHashBits", 2, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 10, 18 }, b.getEntries());
				// addressMap[4] => bucket 3
				b = map.getBucketFromEntryIndex(4);
				assertEquals("localHashBits", 3, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 4, 44, 76 }, b.getEntries());
			}
        }

		/**
		 * Finally, key <code>20</code> is inserted, again into the block
		 * addressed by address table entry <code>100</code>, which is (D).
		 * Since (D) is full, this will cause it to be split. Since there are
		 * d=3 global bits and k=3 local bits for (D), there will be 2^(3-3) ==
		 * 1 entries in the address table for (D). Therefore the address space
		 * must be doubled before (D) may be split. Once the address space has
		 * been doubled, (D) is split into two blocks (D) and (E) having local
		 * bits <code>k=4</code>. The hash table after this expansion looks as
		 * follows:
		 * 
		 * <pre>
		 * [0000] -> (A) [k=3] {32}
		 * [0001] -> (C) [k=1] {23, 9}
		 * [0010] -> (B) [k=2] {18, 10}
		 * [0011] -> (C)
		 * [0100] -> (D) [k=4] {4, 20}
		 * [0101] -> (C)
		 * [0110] -> (B)
		 * [0111] -> (C)
		 *  ---- extension ----
		 * [1000] -> (A)
		 * [1001] -> (C)
		 * [1010] -> (B)
		 * [1011] -> (C)
		 * [1100] -> (E) [k=4] {44, 76}
		 * [1101] -> (C)
		 * [1110] -> (B)
		 * [1111] -> (C)
		 * </pre>
		 */
        {
			{
				assertFalse(map.contains(20));
				map.insert(20);
				System.out.println(map.toString());
				assertEquals("globalHashBits", 4, map.getGlobalHashBits());
				assertEquals("addressSpace", 16, map.getAddressSpaceSize());
				assertEquals("bucketCount", 5, map.getBucketCount());
				assertSameIteratorAnyOrder(new Integer[] { 4, 23, 18, 10, 44,
						32, 9, 76, 20 }, map.getEntries());
				// addressMap[0,8] => bucket 0.
				SimpleBucket b = map.getBucketFromEntryIndex(0);
				assertEquals("addr", map.getAddressFromEntryIndex(0), map
						.getAddressFromEntryIndex(8));
				assertEquals("localHashBits", 3, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 32 }, b
						.getEntries());
				// addressMap[1,3,5,7,9,11,13,15] => bucket 1
				assertEquals("addr", map.getAddressFromEntryIndex(1), map
						.getAddressFromEntryIndex(3));
				assertEquals("addr", map.getAddressFromEntryIndex(1), map
						.getAddressFromEntryIndex(5));
				assertEquals("addr", map.getAddressFromEntryIndex(1), map
						.getAddressFromEntryIndex(7));
				assertEquals("addr", map.getAddressFromEntryIndex(1), map
						.getAddressFromEntryIndex(9));
				assertEquals("addr", map.getAddressFromEntryIndex(1), map
						.getAddressFromEntryIndex(11));
				assertEquals("addr", map.getAddressFromEntryIndex(1), map
						.getAddressFromEntryIndex(13));
				assertEquals("addr", map.getAddressFromEntryIndex(1), map
						.getAddressFromEntryIndex(15));
				b = map.getBucketFromEntryIndex(1);
				assertEquals("localHashBits", 1, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 23, 9 }, b.getEntries());
				// addressMap[2,6,10,14] => bucket 2
				b = map.getBucketFromEntryIndex(2);
				assertEquals("addr", map.getAddressFromEntryIndex(2), map
						.getAddressFromEntryIndex(6));
				assertEquals("addr", map.getAddressFromEntryIndex(2), map
						.getAddressFromEntryIndex(10));
				assertEquals("addr", map.getAddressFromEntryIndex(2), map
						.getAddressFromEntryIndex(14));
				assertEquals("localHashBits", 2, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 10, 18 }, b.getEntries());
				// addressMap[4] => bucket 3
				b = map.getBucketFromEntryIndex(4);
				assertEquals("localHashBits", 4, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 4, 20 }, b.getEntries());
				// addressMap[12] => bucket 5
				b = map.getBucketFromEntryIndex(12);
				assertEquals("localHashBits", 4, b.localHashBits);
				assertSameIteratorAnyOrder(new Integer[] { 44, 76 }, b.getEntries());
			}
        }

    }

	/**
	 * Stress test using distinct integer keys verifies that the
	 * {@link SimpleExtensibleHashMap} correctly tracks the {@link HashMap} for
	 * inserts.
	 * 
	 * @todo Do another test with deletes (or mixed inserts and deletes).
	 */
	public void test_stressTestDistinctKeys() {

		// the #of keys to use.
		final int limit = 100;
		
		// the size of the buckets. smaller buckets involve more structural changes.
		final int bucketSize = 3;
        
		// setup the keys.
		final int[] keys = new int[limit];
		for (int i = 0; i < limit; i++)
			keys[i] = i;
		
		// random permutation of the keys.
		final int[] order = getRandomOrder(limit);
		
		// Ground truth set.
		final Set<Integer> groundTruth = new LinkedHashSet<Integer>(limit);

		// The fixture under test.
        final SimpleExtensibleHashMap map = new SimpleExtensibleHashMap(
                1/* initialCapacity */, bucketSize);

		for (int i = 0; i < limit; i++) {

			final int key = keys[order[i]];

			assertTrue(groundTruth.add(key));

			assertFalse(map.contains(key));
			map.insert(key);
			assertTrue(map.contains(key));

		}
	
		final Integer[] expected = groundTruth.toArray(new Integer[0]);

		assertSameIteratorAnyOrder(expected, map.getEntries());
	
	}
	
}
