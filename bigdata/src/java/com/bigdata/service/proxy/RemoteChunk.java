package com.bigdata.service.proxy;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.io.IStreamSerializer;
import com.bigdata.striterator.IKeyOrder;

/**
 * A chunk of elements materialized from a remote iterator together with some
 * metadata about the state of the remote iterator (whether or not it is
 * exhausted, what its {@link IKeyOrder} is (if any)).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
public class RemoteChunk<E> implements IRemoteChunk<E>, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = -8022644024286873191L;

    /**
     * <code>true</code> iff the source iterator is exhausted.
     */
    private boolean exhausted;

    /**
     * Used to (de-)serialize the chunk of elements.
     */
    private IStreamSerializer<E[]> serializer;
    
    /**
     * The natural order of those elements (if any).
     */
    private IKeyOrder<E> keyOrder;

    /**
     * The chunk of elements.
     */
    private E[] a;

    /**
     * De-serialization ctor.
     */
    public RemoteChunk() {
        
    }
    
    public RemoteChunk(
            final boolean exhausted,
            final IStreamSerializer<E[]> serializer,
            final IKeyOrder<E> keyOrder,
            final E[] a
            ) {

        if (serializer == null)
            throw new IllegalArgumentException();
        
        this.exhausted = exhausted;

        this.serializer = serializer;
        
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

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        // true iff exhausted.
        exhausted = in.readBoolean();

        // true iff an E[] chunk is included.
        final boolean haveChunk = in.readBoolean();
        
        // the natural order of the iterator
        keyOrder = (IKeyOrder<E>)in.readObject();
        
        if(haveChunk) {
            
            // de-serialize the serializer object.
            serializer = (IStreamSerializer<E[]>) in.readObject();
            
            // de-serialize the chunk using that serializer.
            a = serializer.deserialize(in);
            
        }
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeBoolean(exhausted);

        // true iff there are any elements in this chunk.
        final boolean haveChunk = a != null;
        out.writeBoolean(haveChunk);

        // the natural order of the elements (if any).
        out.writeObject(keyOrder);

        if (haveChunk) {

            // The (de-)serializer for the chunk.
            out.writeObject(serializer);

            // serialize the chunk.
            serializer.serialize(out,a);
            
        }

    }

}
