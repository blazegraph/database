package com.bigdata.sparse;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.AbstractTupleFilterator.AtomicRowIterator2;
import com.bigdata.io.SerializerUtil;

/**
 * Helper class for <em>de-serializing</em> logical rows from an
 * {@link AtomicRowIterator2}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TPSTupleSerializer implements ITupleSerializer {

    private static final long serialVersionUID = -2467715806323261423L;

    public static transient final ITupleSerializer INSTANCE = new TPSTupleSerializer(); 
    
    /**
     * De-serializator ctor.
     */
    public TPSTupleSerializer() {
        
    }
    
    public TPS deserialize(ITuple tuple) {

        return (TPS) SerializerUtil.deserialize(tuple.getValueStream());
        
    }

    /**
     * You can get the {@link Schema} and the primary key from
     * {@link #deserialize(ITuple)}.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public Object deserializeKey(ITuple tuple) {

        throw new UnsupportedOperationException();
        
    }

    /**
     * This method is not used since we do not store {@link TPS} objects
     * directly in a {@link BTree}.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public byte[] serializeKey(Object obj) {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * This method is not used since we do not store {@link TPS} objects
     * directly in a {@link BTree}.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public byte[] serializeVal(Object obj) {
        
        throw new UnsupportedOperationException();
        
    }
    
}