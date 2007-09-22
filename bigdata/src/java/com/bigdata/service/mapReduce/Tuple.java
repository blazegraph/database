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
