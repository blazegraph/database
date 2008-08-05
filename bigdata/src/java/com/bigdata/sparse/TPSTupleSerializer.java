package com.bigdata.sparse;

import com.bigdata.btree.BTree;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.io.SerializerUtil;

/**
 * Helper class for (de-)serializing logical rows for {@link AtomicRowFilter}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TPSTupleSerializer extends DefaultTupleSerializer<Void,TPS> {

    private static final long serialVersionUID = -2467715806323261423L;

    public static ITupleSerializer<Void, TPS> newInstance() {
        
        return new TPSTupleSerializer();
        
    }
    
    /**
     * De-serializator ctor.
     */
    public TPSTupleSerializer() {
        
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

    public byte[] serializeVal(TPS obj) {
        
        return SerializerUtil.serialize(obj);
        
    }

    public TPS deserialize(ITuple tuple) {

        return (TPS) SerializerUtil.deserialize(
//                tuple.getValueStream()
                tuple.getValue() // FIXME use getValueStream() instead - serialization problem
                );
        
    }

    /**
     * You can get the {@link Schema} and the primary key from
     * {@link #deserialize(ITuple)}.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public Void deserializeKey(ITuple tuple) {

        throw new UnsupportedOperationException();
        
    }
    
}
