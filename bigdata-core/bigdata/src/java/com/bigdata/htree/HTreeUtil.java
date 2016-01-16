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

import it.unimi.dsi.bits.Fast;

import com.bigdata.util.BytesUtil;

/**
 * Static utility methods and data for an {@link HTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HTreeUtil {

    /**
     * Find the first power of two which is GTE the given value. This is used to
     * compute the size of the address space (in bits) which is required to
     * address a hash table with slots for that many buckets.
     */
    static int getMapSize(final int nentries) {

        if (nentries <= 0)
            throw new IllegalArgumentException();

        int i = 1;

        while ((1 << i) < nentries)
            i++;

        return i;

    }

	/**
     * Return <code>2^n</code>.
     * 
     * @param n
     *            The exponent.
     *            
     * @return The result.
     */
    static int pow2(final int n) {
        
//      return (int) Math.pow(2d, n);
        return 1 << n;
        
    }

    /**
     * Return <code>true</code> if the argument is a power of TWO (2).
     * 
     * @param v
     *            The argument.
     *            
     * @return <code>true</code> if the argument is a power of TWO (2).
     */
    static public boolean isPowerOf2(final int v) {

        return ((v & -v) == v);

    }

	/**
	 * Return the #of entries in the address map for a page having the given
	 * local depth. This is <code>2^(globalHashBits - localHashBits)</code>. The
	 * following table shows the relationship between the global hash bits (gb),
	 * the local hash bits (lb) for a page, and the #of directory entries for
	 * that page (nentries).
	 * 
	 * <pre>
	 * gb  lb   nentries
	 * 1    0   2
	 * 1    1   1
	 * 2    0   4
	 * 2    1   2
	 * 2    2   1
	 * 3    0   8
	 * 3    1   4
	 * 3    2   2
	 * 3    3   1
	 * 4    0   16
	 * 4    1   8
	 * 4    2   4
	 * 4    3   2
	 * 4    4   1
	 * </pre>
	 * 
	 * @param globalDepth
	 *            The global depth of the parent.
	 * @param localDepth
	 *            The local depth of the child.
	 * 
	 * @return The #of directory entries for that child page.
	 * 
	 * @throws IllegalArgumentException
	 *             if either argument is less than ZERO (0).
	 * @throws IllegalArgumentException
	 *             if <i>localHashBits</i> is greater than
	 *             <i>globalHashBits</i>.
	 */
	public static int getSlotsOnPage(final int globalDepth, final int localDepth) {

		if (localDepth < 0)
			throw new IllegalArgumentException();

		if (globalDepth < 0)
			throw new IllegalArgumentException();

		if (localDepth > globalDepth)
			throw new IllegalArgumentException();

		// The #of slots entries for the child page.
		final int numSlotsForPage = (1 << (globalDepth - localDepth));

		return numSlotsForPage;

	}

	/**
	 * Return the local depth of a child page having <i>npointers</i> to that
	 * page in the parent node. This method is based on the relationship
	 * <code>2^(n-i) = npointers</code> where <i>n</i> is the global depth of
	 * the parent and <i>i</i> is the local depth of the child. The value of
	 * <i>i</i> is the MSB of <code>(npointers / (2^n))</code>. That equation
	 * always evaluates to a power of two since npointers is a power of two. The
	 * MSB is the index of the highest (and only) ONE (1) bit in the result for
	 * that equation.
	 * 
	 * @param addressBits
	 *            The #of address bits for the hash tree. This is set when the
	 *            tree is configured and is immutable. <i>addressBits</i> is not
	 *            required for the computation, but is used to validate the
	 *            other arguments.
	 * @param globalDepth
	 *            The global depth of the parent node.
	 * @param npointers
	 *            The #of pointers to that child in the parent node. All of
	 *            those pointers will be within a single buddy hash table in the
	 *            parent. The maximum value for <i>npointers</i> is
	 *            <code>2^globalDepth</code> for a given <i>globalDepth</i>.
	 *            (The maximum value of <i>globalDepth</i> is the configured
	 *            value of <i>addressBits</code> for the hash tree.)
	 * 
	 * @return The local depth of that child node.
	 * 
	 * @throws IllegalArgumentException
	 *             if <i>addressBits</i> is LT ONE (1).
	 * @throws IllegalArgumentException
	 *             if <i>globalDepth</i> is LT ZERO (0).
	 * @throws IllegalArgumentException
	 *             if <i>globalDepth</i> is GT <code>2^addressBits</code>.
	 * @throws IllegalArgumentException
	 *             if <i>npointers</i> is LT ONE (1).
	 * @throws IllegalArgumentException
	 *             if <i>npointers</i> is greater than
	 *             <code>2^globalDepth</code>.
	 * @throws IllegalArgumentException
	 *             if <i>npointers</i> is not a power of 2.
	 */
	public static int getLocalDepth(final int addressBits,
			final int globalDepth, final int npointers) {

		if (addressBits < 1)
			throw new IllegalArgumentException();

		if (globalDepth < 0)
			throw new IllegalArgumentException();
		if (globalDepth > (1 << addressBits)) // aka 2^addressBits
			throw new IllegalArgumentException();

		if (npointers < 1)
			throw new IllegalArgumentException();
		if (npointers > (1 << globalDepth)) // aka 2^globalDepth
			throw new IllegalArgumentException();
        if ((npointers & -npointers) != npointers)  // e.g., not a power of 2.
        	throw new IllegalArgumentException();
		
		final int x = (1 << globalDepth) / npointers;
		
		final int i = Fast.mostSignificantBit(x);
		
		assert 1 << (globalDepth-i) == npointers;
		
//		System.err.println("addressBits=" + addressBits + ", globalDepth="
//				+ globalDepth + ", npointers=" + npointers + ", x=" + x
//				+ ", localDepth=" + i);
		
// Alternative code from Martyn. We have not compared performance.  I suspect
// that the relatively "closed" form solution above will do better, but this
// should be tested.
//		      
// npointers == 2^(g-i)
// 2^i = (2^g)/npointers
// need to find nb st 2^nb == npointers
// then 2^i == 2^g / 2^nb = 2^(g-nb)
// and i == g-nb
//  
//  int nb = 0;
//  while (npointers != (1 << nb)) ++nb;
//  
//  return globalDepth - nb;
		        
		return i;
		
    }

    /**
     * Find the offset of the buddy hash table or buddy bucket in the child.
     * Each page of the hash tree is logically an ordered array of "buddies"
     * sharing the same physical page. When the page is a directory page, the
     * buddies are buddy hash tables. When the page is a bucket page, the
     * buddies are buddy hash buckets. The returned value is the offset required
     * to index into the appropriate buddy hash table or buddy hash bucket in
     * the child page.
     * 
     * @param hashBits
     *            The relevant bits of the hash code used to lookup the child
     *            within the parent.
     * @param globalDepth
     *            The global depth of the parent.
     * @param localDepth
     *            The local depth of the child page within the parent.
     * 
     * @return The offset of the start of the buddy hash table or buddy hash
     *         bucket within the child. This is an index into the slots of the
     *         child. There are m:=(2^addressBits)-1 slots in the child, so the
     *         returned index is in the half open range [0:m).
     * 
     * @throws IllegalArgumentException
     *             if the <i>globalDepth</i> is negative.
     * @throws IllegalArgumentException
     *             if the <i>localDepth</i> is greater than the
     *             <i>globalDepth</i>.
     */
	public static int getBuddyOffset(final int hashBits, final int globalDepth,
			final int localDepth) {

		if (globalDepth < 0)
			throw new IllegalArgumentException();

		if (localDepth > globalDepth)
			throw new IllegalArgumentException();

		final int nbits = (globalDepth - localDepth);

		// #of address slots in each buddy hash table in the child.
		final int slotsPerBuddy = (1 << localDepth);

		// index of the buddy in [0:nChildBuddies-1].
		final int tmp = BytesUtil.maskOffLSB(hashBits, nbits);

		// index of the buddy as a slot offset into the child.
		final int childBuddyOffset = tmp * slotsPerBuddy;
		
		return childBuddyOffset;

	}

}
