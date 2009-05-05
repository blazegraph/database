package com.bigdata.service.ndx.pipeline;

import com.bigdata.btree.keys.KVO;

/**
 * Extends {@link KVO} to provide handshaking with a {@link KVOLatch}.
 * <p>
 * Note: A duplicate remover MUST NOT eliminate "duplicate" {@link KVOC}s when
 * one or the other has {@link KVOLatch} not shared by the other. Eliminating a
 * "duplicate" in this case would cause a {@link KVOLatch#dec()} to be "lost"
 * and generally results in non-termination of {@link KVOLatch#await()} since
 * the count for the "lost" latch would never reach zero.
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
