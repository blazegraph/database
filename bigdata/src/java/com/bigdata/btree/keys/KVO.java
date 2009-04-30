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
 * Created on Aug 12, 2008
 */

package com.bigdata.btree.keys;

import com.bigdata.btree.BytesUtil;
import com.bigdata.service.ndx.pipeline.KVOC;

/**
 * A key-value-object tuple. Comparison places the {@link KVO} tuples into an
 * order based on the interpretation of their {@link #key}s as unsigned
 * byte[]s. This may be used to perform a correlated sort of keys and values.
 * This class may also be used to pair an optional unserialized representation
 * of the value {@link Object} with the unsigned byte[] key and the serialized
 * byte[] value.
 * 
 * @param <O>
 *            The generic type of the unserialized value object.
 *            
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class KVO<O> implements Comparable<KVO<O>>{

    /**
     * The unsigned byte[] key (required).
     */
    public final byte[] key;

    /**
     * The serialized byte[] value (optional).
     */
    public final byte[] val;

    /**
     * The unserialized object value (optional, even when {@link #val} is
     * given).
     */
    public final O obj;

    /**
     * 
     * @param key
     *            The unsigned byte[] key (required).
     * @param val
     *            The serialized byte[] value (optional).
     */
    public KVO(final byte[] key, final byte[] val) {

        this(key, val, null/* obj */);

    }

    /**
     * 
     * @param key
     *            The unsigned byte[] key (required).
     * @param val
     *            The serialized byte[] value (optional).
     * @param obj
     *            The unserialized object value (optional, even when <i>val</i>
     *            is given).
     */
    public KVO(final byte[] key, final byte[] val, final O obj) {

        if (key == null)
            throw new IllegalArgumentException();

        this.key = key;

        this.val = val;

        this.obj = obj;

    }
    
    /**
     * Method is invoked when the tuple represented by the {@link KVO} has been
     * written onto the index by an <em>asynchronous</em> write operation.
     * {@link KVOC} overrides this method to support notification when
     * all tuples generated within some scope have been written onto the
     * database.
     */
    public void done() {
        
    }

    public int compareTo(final KVO<O> arg0) {

        return BytesUtil.compareBytes(key, arg0.key);

    }

    public String toString() {

        return "KVO{key=" + BytesUtil.toString(key) + ", val=" + val + ", obj="
                + obj + "}";

    }

    /**
     * Return a dense array of the keys in a {@link KVO}[] chunk. The keys are
     * copied by reference, not by value.
     * 
     * @param chunk
     *            A chunk of {@link KVO} objects.
     * 
     * @return The keys.
     */
    static public byte[][] getKeys(final KVO[] chunk) {

        if (chunk == null)
            throw new IllegalArgumentException();
        
        final byte[][] keys = new byte[chunk.length][];
        
        for (int i = 0; i < chunk.length; i++) {

            keys[i] = chunk[i].key;
            
        }
        
        return keys;
        
    }

    /**
     * Return a dense array of the values in a {@link KVO}[] chunk. The values
     * are copied by reference, not by value.
     * 
     * @param chunk
     *            A chunk of {@link KVO} objects.
     * 
     * @return The values.
     */
    static public byte[][] getVals(final KVO[] chunk) {

        if (chunk == null)
            throw new IllegalArgumentException();

        final byte[][] vals = new byte[chunk.length][];

        for (int i = 0; i < chunk.length; i++) {

            vals[i] = chunk[i].val;

        }

        return vals;

    }

    /**
     * Return a dense array. If the array is already dense, then the array
     * reference is returned. This is not a deep copy.
     * 
     * @param a
     *            The array.
     * @param len
     *            The #of elements in the array [0:len-1].
     * @return A dense array.
     */
    static public <T> KVO<T>[] dense(final KVO<T>[] a, final int len) {

        if (a == null)
            throw new IllegalArgumentException();

        if (len < 0 || len > a.length)
            throw new IllegalArgumentException();

        if (len == a.length) // perfect fit.
            return a;

        final KVO<T>[] b = new KVO[len];

        System.arraycopy(a, 0/*srcpos*/, b, 0/*dstpos*/, len);

        return b;

    }

}
