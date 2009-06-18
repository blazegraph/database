package com.bigdata.service.ndx.pipeline;

/**
 * Extends {@link KVOList} to provide handshaking with a {@link KVOLatch}.
 * <p>
 * Note: {@link IDuplicateRemover}s MUST create a list from the identified
 * duplicates so that the {@link KVOLatch} of each duplicate as well as the
 * original are decremented after a successful write. Failure to do this will
 * cause a {@link KVOLatch#dec()} to be "lost" and generally results in
 * non-termination of {@link KVOLatch#await()} since the count for the "lost"
 * latch would never reach zero.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <O>
 *            The generic type of the unserialized value object.
 */
public class KVOC<O> extends KVOList<O> {

    /** The latch. */
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
     * Extended to decrement the {@link KVOLatch}. This inherits from
     * {@link KVOList}, which maps {@link KVOList#done()} over the list of
     * duplicates. This ensures that all latches are decremented once the
     * original value has been successfully written onto an index partition.
     */
    @Override
    public void done() {

        super.done();
        
        latch.dec();

    }

}
