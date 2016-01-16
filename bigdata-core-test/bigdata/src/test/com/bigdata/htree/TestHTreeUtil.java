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
/*
 * Created on Apr 20, 2011
 */

package com.bigdata.htree;

import java.util.Formatter;

import junit.framework.TestCase2;

/**
 * Unit tests for {@link HTreeUtil}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHTreeUtil extends TestCase2 {

    /**
     * 
     */
    public TestHTreeUtil() {
    }

    /**
     * @param name
     */
    public TestHTreeUtil(String name) {
        super(name);
    }

    /**
     * Unit test for {@link HTreeUtil#getMapSize(int)}.
     */
    public void test_getMapSize() {
        
        assertEquals(1/* addressSpaceSize */, HTreeUtil.getMapSize(1)/* fanout */);
        assertEquals(1/* addressSpaceSize */, HTreeUtil.getMapSize(2)/* fanout */);
        assertEquals(2/* addressSpaceSize */, HTreeUtil.getMapSize(3)/* fanout */);
        assertEquals(2/* addressSpaceSize */, HTreeUtil.getMapSize(4)/* fanout */);
        assertEquals(3/* addressSpaceSize */, HTreeUtil.getMapSize(5)/* fanout */);
        assertEquals(3/* addressSpaceSize */, HTreeUtil.getMapSize(6)/* fanout */);
        assertEquals(3/* addressSpaceSize */, HTreeUtil.getMapSize(7)/* fanout */);
        assertEquals(3/* addressSpaceSize */, HTreeUtil.getMapSize(8)/* fanout */);
        assertEquals(4/* addressSpaceSize */, HTreeUtil.getMapSize(9)/* fanout */);

        assertEquals(5/* addressSpaceSize */, HTreeUtil.getMapSize(32)/* fanout */);

        assertEquals(10/* addressSpaceSize */, HTreeUtil.getMapSize(1024)/* fanout */);

    }

    /** Unit test for {@link HTreeUtil#isPowerOf2(int)}. */
    public void test_isPowerOf2() {

        for (int i = 0; i < 32; i++) {

            final int v = 1<<i;
            
            assertTrue(HTreeUtil.isPowerOf2(v));

            if (v > 1)
                assertFalse(HTreeUtil.isPowerOf2(v + 1));
            
        }
        
    }
    
	/**
	 * Prints various tables and runs consistency tests on the htree math
	 * operations dealing with addressBits, globalDepth, localDepth, etc.
	 * <p>
	 * <dl>
	 * <dt>addressBits</dt>
	 * <dd>Size of the address space (fixed for a given hash tree).</dd>
	 * <dt>globalDepth</dt>
	 * <dd>The size of the address space for a buddy hash table on a directory
	 * page.</dd>
	 * <dt>nbuddies</dt>
	 * <dd>The #of buddy tables (buckets) on a directory (bucket) page. This
	 * depends solely on <i>addressBits</i> (a constant) and <i>globalDepth</i>.
	 * </dd>
	 * <dt>slots/buddy</dt>
	 * <dd>The #of directory entries in a buddy hash table. This depends solely
	 * on <i>globalDepth</i>.</dd>
	 * <dt>npointers</dt>
	 * <dd>The #of pointers in the parent which point to a given child (given
	 * based on a scan of the parent).</dd>
	 * <dt>localDepth</dt>
	 * <dd>The #of bits in the key examined at a given level in the tree. The
	 * range is [0:addressBits]. This depends solely on the <i>globalDepth</i>
	 * of a directory page and the #of pointers to child (<i>npointers</i>) in
	 * that directory page.</dd>
	 * <dt>nbits</dt>
	 * <dd>This is <code>(globalDepth-localDepth)</code>. It is the #of bits of
	 * the hash code which will be used to compute the <i>buddyOffset</i>.</dd>
	 * <dt>hashBits</dt>
	 * <dd>The LSB <code>globalDepth-localDepth</code> bits of the hash code.
	 * This is used to compute the <i>buddyOffset</i>.</dd>
	 * <dt>buddyOffset</dt>
	 * <dd>The offset of the buddy hash table or buddy bucket within the child.</dd>
	 * </dl>
	 */
    private void doHTreeMath(final int addressBits, final StringBuilder sb) {

		final Formatter f = new Formatter(sb);

		f.format("\n%12s %12s %12s %12s %12s %12s %5s %10s %12s",// %4s %12s", //
				"addressBits", "globalDepth", //
				"nbuddies", "#slots/buddy", //
				"npointers", "localDepth", //
				"nbits", "hashBits", "buddyOffset"//
//				"chk", "pass"//
				);

		int nfail = 0;
		
		for (int globalDepth = 0; globalDepth <= addressBits; globalDepth++) {

			// #of buddy tables on a page.
			final int nbuddies = (1 << addressBits) / (1 << globalDepth);

			// #of address slots in each buddy hash table.
			final int slotsPerBuddy = (1 << globalDepth);

			for (int npointers = (1 << globalDepth); npointers >= 1; npointers--) {

				if ((npointers & -npointers) != npointers) // not a power of 2
					continue;

				final int localDepth = HTreeUtil.getLocalDepth(addressBits,
						globalDepth, npointers);

				if (localDepth < 0) {
					log.error("localDepth must be non-negative.");
					nfail++;
				}

				if (localDepth > globalDepth) {
					log.error("localDepth must be LTE globalDepth.");
					nfail++;
				}

				// cross check on localDepth by computing npointers.
				final int chk = 1 << (globalDepth - localDepth);

				if (chk != npointers) {
					log.error("npointers cross check failed.");
					nfail++;
				}

				/*
				 * Find the buddy offset for a sequence of relevant hash bits
				 * having a 1 in each of the bit positions to which the buddy
				 * indexing is sensitive given the globalDepth of the parent and
				 * the localDepth of the child. This gives us an overview of the
				 * buddy indexing without requiring us to print off a table for
				 * each distinct (globalDepth-localDepth) bit integer.
				 */

//				// #of buddy tables on the child page (same formula as above,
//				// but using the local depth of the child rather than global
//				// depth of the parent).
//				final int nChildBuddies = (1 << addressBits) / (1 << localDepth);

//				// #of address slots in each buddy hash table (same formula as
//				// above, but using the local depth of the child rather than the
//				// global depth of the parent).
//				final int slotsPerChildBuddy = (1 << localDepth);

//				// total #of address slots in the child page across all buddies
//				// on that page.
//				final int childSlots = nChildBuddies * slotsPerChildBuddy;
				
				final int NBITS = (globalDepth - localDepth);
				
				for (int nbits = 0; nbits <= NBITS; nbits++) {
				
					// zero for zero bits. otherwise 1<<(nbits-1).
					final int hashBits = nbits == 0 ? 0 : (1 << nbits - 1);

					final int buddyOffset = HTreeUtil.getBuddyOffset(hashBits,
							globalDepth, localDepth);

					if (buddyOffset < 0) {
						log.error("buddyOffset is negative.");
						nfail++;
					}

					if (buddyOffset >= (1 << addressBits)) {
						log.error("buddyOffset too large.");
						nfail++;
					}

					f.format(
							"\n%12d %12d %12d %12d %12d %12d %5d %10d %12d",// %12d %4s", //
							addressBits, globalDepth, nbuddies, slotsPerBuddy,
							npointers, localDepth,//
							/*childSlots,*/ nbits, hashBits, buddyOffset
//							chk, chk == npointers ? "yes" : "no"//
					);

				}
				
			}

		}

		f.flush();

		assertEquals("nfail", 0, nfail);

	}

	/**
	 * Exercise the hash tree math for a 1-bit address space. This is the
	 * absolute minimum size for an address space and the address space is only
	 * capable of making a single bit distinction per level in the hash tree.
	 */
	public void test_htree_math_addressBits_1() {

		final int addressBits = 1;
		
		final StringBuilder sb = new StringBuilder();

		doHTreeMath(addressBits, sb);

		if (log.isInfoEnabled())
			log.info(sb.toString());

	}

    /**
     * Exercise the hash tree math for a 2-bit address space.
     */
	public void test_htree_math_addressBits_2() {

		final int addressBits = 2;
		
		final StringBuilder sb = new StringBuilder();

		doHTreeMath(addressBits, sb);

		if (log.isInfoEnabled())
			log.info(sb.toString());

	}

    /**
     * Exercise the hash tree math for a 4-bit address space.
     */
	public void test_htree_math_addressBits_4() {

		final int addressBits = 4;
		
		final StringBuilder sb = new StringBuilder();

		doHTreeMath(addressBits, sb);

		if (log.isInfoEnabled())
			log.info(sb.toString());

	}

    /**
     * Exercise the hash tree math for an 8-bit address space.
     */
	public void test_htree_math_addressBits_8() {

		final int addressBits = 8;
		
		final StringBuilder sb = new StringBuilder();

		doHTreeMath(addressBits, sb);

		if (log.isInfoEnabled())
			log.info(sb.toString());

	}

	/**
	 * Exercise the hash tree math for an 10-bit address space (this corresponds
	 * to a 4k page).
	 */
	public void test_htree_math_addressBits_10() {

		final int addressBits = 10;
		
		final StringBuilder sb = new StringBuilder();

		doHTreeMath(addressBits, sb);

		if (log.isInfoEnabled())
			log.info(sb.toString());

	}

	/**
	 * Exercise the hash tree math for an 11-bit address space (this corresponds
	 * to an 11k page).
	 */
	public void test_htree_math_addressBits_11() {

		final int addressBits = 11;
		
		final StringBuilder sb = new StringBuilder();

		doHTreeMath(addressBits, sb);

		if (log.isInfoEnabled())
			log.info(sb.toString());

	}

	/**
	 * Unit test for {@link HTreeUtil#getBuddyOffset(int, int, int)} for the
	 * case where globalDepth:=2 and localDepth:=0. For this case, the child
	 * will have 4 buddies and the (globalDepth-localDepth) lower bits of the
	 * int32 hash code will be used to index into those buddies.
	 */
	public void test_getBuddyOffset_globalDepth2_localDepth0() {

		final int globalDepth = 2;

		final int localDepth = 0;

		assertEquals(0, HTreeUtil.getBuddyOffset(0x00/* 00 */, globalDepth,
				localDepth));
		
		assertEquals(1, HTreeUtil.getBuddyOffset(0x01/* 01 */, globalDepth,
				localDepth));
		
		assertEquals(2, HTreeUtil.getBuddyOffset(0x02/* 10 */, globalDepth,
				localDepth));
		
		assertEquals(3, HTreeUtil.getBuddyOffset(0x03/* 11 */, globalDepth,
				localDepth));

	}
	
	public void test_getBuddyOffset_globalDepth2_localDepth1() {

		final int globalDepth = 2;

		final int localDepth = 1;

		assertEquals(0, HTreeUtil.getBuddyOffset(0x00/* 00 */, globalDepth,
				localDepth));
		
		assertEquals(2, HTreeUtil.getBuddyOffset(0x01/* 01 */, globalDepth,
				localDepth));
		
	}
	
}
