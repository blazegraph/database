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

import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.btree.keys.TestKeyBuilder;

/**
 * Generate <i>n</i> random and distinct URIs. This distribution has a shared
 * prefix followed by some random bytes representing an integer value. There are
 * no <code>null</code>s, even for the B+Tree values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RandomURIGenerator implements IRabaGenerator {

    /** Yes. */
    public boolean isKeysGenerator() {
        return true;
    }

    /** Yes. */
    public boolean isValuesGenerator() {
        return true;
    }

    private final Random r;
    
    public RandomURIGenerator(final Random r) {
    
        this.r = r;
        
    }
    
    public byte[][] generateKeys(int size) {
        
        final byte[][] data = getRandomURIs(size, r);

        // put into sorted order.
        Arrays.sort(data, 0, data.length, UnsignedByteArrayComparator.INSTANCE);

        return data;
        
    }

    public byte[][] generateValues(int size) {
        
        final byte[][] data = getRandomURIs(size, r);
        
        return data;
        
    }
    
    /**
     * Generate <i>n</i> random and distinct URIs. This distribution has a
     * shared prefix followed by some random bytes representing an integer
     * value.
     * 
     * @return Distinct but <em>unordered</em> URIs coded as unsigned byte[]s.
     */
    byte[][] getRandomURIs(final int n, final Random r) {

        final byte[][] data = new byte[n][];

        final String ns = "http://www.bigdata.com/rdf#";

        long lastCounter = r.nextInt();

        for (int i = 0; i < data.length; i++) {

            data[i] = TestKeyBuilder.asSortKey(ns + String.valueOf(lastCounter));

            final int inc = r.nextInt(100) + 1;

            assert inc > 0 : "inc=" + inc;

            lastCounter += inc;

        }

        return data;

    }

}
