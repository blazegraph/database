package com.bigdata.service.ndx.pipeline;

import com.bigdata.btree.keys.KVO;

/**
 * Extends {@link KVO} to provide handshaking with a {@link KVOLatch}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <O>
 *            The generic type of the unserialized value object.
 */
public class KVOC<O> extends KVO<O> {

    private final KVOLatch latch;

    /**
     * 
     * @param key
     *            The unsigned byte[] key (required).
     * @param val
     *            The byte[] value (optional).
     * @param obj
     *            The paired application object (optional).
     * @param latch
     *            The object that maintains the counter.
     */
    public KVOC(final byte[] key, final byte[] val, final O obj,
            final KVOLatch latch) {

        super(key, val, obj);

        if (latch == null)
            throw new IllegalArgumentException();
        
        latch.inc();

        this.latch = latch;

    }

    /**
     * Decrements the {@link KVOLatch}.
     */
    public void done() {

        latch.dec();

    }

}
