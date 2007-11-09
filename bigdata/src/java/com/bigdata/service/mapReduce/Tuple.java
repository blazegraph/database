/*

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
package com.bigdata.service.mapReduce;

import com.bigdata.btree.BytesUtil;

/**
 * A key-value pair that is an output from an {@link IMapTask}. Instances of
 * this class know how to place themselves into a <em>unsigned</em> byte[]
 * order based on their {@link #key}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class Tuple implements Comparable<Tuple> {
    /**
     * The reduce input partition into which this tuple is placed by its
     * {@link #key}, the {@link IHashFunction} for the {@link IMapReduceJob},
     * and the #of reduce tasks to be run.
     */
    final int partition;

    /**
     * The complete key (application key, map task UUID, and map task local
     * tuple counter).
     */
    final byte[] key;

    /**
     * The value.
     */
    final byte[] val;

    Tuple(int partition, byte[] key, byte[] val) {
        this.partition = partition;
        this.key = key;
        this.val = val;
    }

    public int compareTo(Tuple arg0) {
        return BytesUtil.compareBytes(key, arg0.key);
    }
}
