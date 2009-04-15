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

    public int compareTo(final KVO<O> arg0) {

        return BytesUtil.compareBytes(key, arg0.key);

    }

    public String toString() {

        return "KVO{key=" + BytesUtil.toString(key) + ", val=" + val + ", obj="
                + obj + "}";

    }

}
