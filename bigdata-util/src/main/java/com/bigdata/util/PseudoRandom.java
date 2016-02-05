/*

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

package com.bigdata.util;

/**
 * PseudoRandom is derived from an algorithm used to create a dissolve
 * graphics effect.
 * 
 * Given a maximum number it generates all numbers from 1 to that number
 * in a deterministic but pseudorandom order.
 * 
 * It is therefore particularly useful for test cases.
 * 
 * See: http://www.mactech.com/articles/mactech/Vol.06/06.12/SafeDissolve/index.html
 * 
 */

public class PseudoRandom {
	final private int m_mask;// = 0;
	private int m_next = 1;
	final private int m_max;
	
	private static final int[] s_masks = {
	0x03, 0x06, 0x0C, 0x14, 0x30, 0x60, 0xB8, 0x0110, 0x0240, 
	0x0500, 0x0CA0, 0x1B00, 0x3500, 0x6000, 0xB400, 0x00012000, 0x00020400, 
	0x00072000, 0x00090000, 0x00140000, 0x00300000, 0x00400000, 0x00D80000, 
	0x01200000, 0x03880000, 0x07200000, 0x09000000, 0x14000000, 0x32800000, 
	0x48000000, 0xA3000000};
	
    /**
     * Create a pseudo random number generator which will visit each integer in
     * a half-open range.
     * 
     * @param range
     *            The exclusive upper bound of the half-open range (0:range].
     * @param next
     *            The next number to visit in (0:range].
     */
	public PseudoRandom(final int range, final int next) {

	    this(range);

        if (next < 0 || next >= range)
            throw new IllegalArgumentException();

        m_next = next + 1;

	}
	
    /**
     * Create a pseudo random number generator which will visit each integer in
     * a half-open range. The first visited value will be <code>1</code>.
     * 
     * @param range
     *            The exclusive upper bound of the half-open range (0:range].
     */
	public PseudoRandom(final int range) {
	    int m_mask = 0;
		for (int m = 0; m < s_masks.length; m++) {
			if (s_masks[m] > range) {
				m_mask = s_masks[m];
				break;
			}
		}
		this.m_mask = m_mask;
		m_max = range;
	}
	
	public int next() {
		if ((m_next & 1) == 1)
			m_next = (m_next >> 1) ^ m_mask;
		else m_next = (m_next >> 1);
		
		if (m_next > m_max)
			return next();
		else			
			return m_next - 1;
	}
	
    /**
     * The next number modulo the range.
     * 
     * @param range
     *            The range.
     * @return The number.
     * 
     * @throws IllegalArgumentException
     *             if the range is negative.
     * @throws IllegalStateException
     *             if the range is greater than the range specified to the
     *             constructor (ie., the generated could not produce all numbers
     *             in the given range).
     */
    public int nextInt(final int range) {
        if (range == 0)
            return 0;
        
        if (range < 0)
            throw new IllegalArgumentException();
        if (range > m_max)
            throw new IllegalStateException(
                    "Range exceeds max range of generator");
        
        return next() % range;
    }

    /**
     * Reset the pseudo random generator to a state where it has just visited
     * <i>prev</i>.
     * 
     * @param prev
     *            The "previous" value.
     * 
     * @return The next value in the pseudo random sequence after <i>prev</i>.
     */
    public int next(final int prev) {

        if (prev < 0 || prev >= m_max)
            throw new IllegalArgumentException();

        m_next = prev + 1;

        return next();

	}

	public void nextBytes(byte[] key, final int prev) {
		m_next = prev;
		
		nextBytes(key);
	}

	public void nextBytes(byte[] key) {
		for (int i = 0; i < key.length; i++) {
			key[i] = (byte) next();
		}
	}
}
