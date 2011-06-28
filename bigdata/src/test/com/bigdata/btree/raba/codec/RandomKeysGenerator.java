/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Aug 28, 2009
 */

package com.bigdata.btree.raba.codec;

import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import com.bigdata.btree.BytesUtil;

/**
 * Random B+Tree keys generator. The keys are variable length unsigned byte[]s
 * of up to <i>maxKeyLength</i> bytes. Random byte[]s are generated and then
 * sorted into unsigned byte[] order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RandomKeysGenerator implements IRabaGenerator {

    /** Yes. */
    public boolean isKeysGenerator() {
        return true;
    }

    /** No. */
    public boolean isValuesGenerator() {
        return false;
    }

    private final Random r;
    private final int maxKeys;
    private final int maxKeyLength;

    /**
     * 
     * @param r
     *            The random number generator.
     * 
     * @param maxKeys
     *            The branching factor (will be the capacity of the array).
     * 
     * @param maxKeyLength
     *            The maximum length of a key.
     */
    public RandomKeysGenerator(final Random r, final int maxKeys,
            final int maxKeyLength) {

        assert maxKeys > 0;
        assert maxKeyLength > 0;
        
        this.r = r;
        this.maxKeys = maxKeys;
        this.maxKeyLength = maxKeyLength;
        
    }

    /**
     * Generate a set of N random distinct byte[] keys in sorted order using an
     * unsigned byte[] comparison function.
     * 
     * @param nkeys
     *            The #of keys to generate.
     * 
     * @return A byte[][] with nkeys non-null byte[] entries and a capacity of
     *         maxKeys.
     */
    public byte[][] generateKeys(final int nkeys) {

        if (nkeys < 0)
            throw new IllegalArgumentException();

        if (nkeys > maxKeys)
            throw new IllegalArgumentException();

        /*
         * Generate maxKeys distinct keys (sort requires that the keys are
         * non-null).
         */

        // used to ensure distinct keys.
        final Set<byte[]> set = new TreeSet<byte[]>(
                BytesUtil.UnsignedByteArrayComparator.INSTANCE);

        final byte[][] keys = new byte[maxKeys][];

        int n = 0;

        while (n < maxKeys) {

            // random key length in [1:maxKeyLen].
            final byte[] key = new byte[r.nextInt(maxKeyLength) + 1];

            // random data in the key.
            r.nextBytes(key);

            if (set.add(key)) {

                keys[n++] = key;

            }

        }

        /*
         * place keys into sorted order.
         */
        Arrays.sort(keys, BytesUtil.UnsignedByteArrayComparator.INSTANCE);

        /*
         * clear out keys from keys[nkeys] through keys[maxKeys-1].
         */
        for (int i = nkeys; i < maxKeys; i++) {

            keys[i] = null;

        }

        return keys;

    }

    public byte[][] generateValues(int size) {

        throw new UnsupportedOperationException();

    }

}
