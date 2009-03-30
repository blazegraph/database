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
 * Unlike {@link KV}, this class also pairs an unserialized {@link Object} with
 * the unsigned byte[] key and the serialized byte[] value.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class KVO<O> implements Comparable<KVO<O>>{
    
    public byte[] key;
    public byte[] val;
    public O obj;
    
    public KVO(final byte[] key, final byte[] val, O obj) {
        
        this.key = key;
        
        this.val = val;
        
        this.obj = obj;
        
    }

    public int compareTo(KVO<O> arg0) {

        return BytesUtil.compareBytes(key, arg0.key);
        
    }

    public String toString() {
        
        return "KVO{key=" + BytesUtil.toString(key) + ", val=" + val + ", obj="
                + obj + "}";
        
    }
    
}
