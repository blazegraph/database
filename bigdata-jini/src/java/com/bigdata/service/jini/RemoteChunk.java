package com.bigdata.service.jini;

import com.bigdata.btree.IDataSerializer;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.striterator.IRemoteChunk;

/**
 * A chunk of elements materialized from a remote iterator together with some
 * metadata about the state of the remote iterator (whether or not it is
 * exhausted, what its {@link IKeyOrder} is (if any)).
 * 
 * @todo custom serialization and compression of elements in a chunk along the
 *       same lines as {@link IDataSerializer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
class RemoteChunk<E> implements IRemoteChunk<E> {

    /**
     * 
     */
    private static final long serialVersionUID = -8022644024286873191L;

    private final boolean exhausted;

    private final IKeyOrder<E> keyOrder;

    private final E[] a;

    public RemoteChunk(final boolean exhausted, final IKeyOrder<E> keyOrder,
            E[] a) {

        this.exhausted = exhausted;

        this.keyOrder = keyOrder; // MAY be null.

        this.a = a; // MUST be null if no elements to be sent.

    }

    public E[] getChunk() {

        return a;

    }

    public IKeyOrder<E> getKeyOrder() {

        return keyOrder;

    }

    public boolean isExhausted() {

        return exhausted;

    }

}
