package com.bigdata.sparse;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.btree.BTree;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.keys.IKeyBuilderFactory;
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
        
        return new TPSTupleSerializer(getDefaultKeyBuilderFactory());
        
    }
    
    /**
     * De-serializator ctor.
     */
    public TPSTupleSerializer() {
        
    }

    public TPSTupleSerializer(IKeyBuilderFactory keyBuilderFactory) {
        
        super(keyBuilderFactory);
        
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
    
    /**
     * The initial version (no additional persistent state).
     */
    private final static transient byte VERSION0 = 0;

    /**
     * The current version.
     */
    private final static transient byte VERSION = VERSION0;

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        super.readExternal(in);
        
        final byte version = in.readByte();
        
        switch (version) {
        case VERSION0:
            break;
        default:
            throw new UnsupportedOperationException("Unknown version: "
                    + version);
        }

    }

    public void writeExternal(final ObjectOutput out) throws IOException {

        super.writeExternal(out);
        
        out.writeByte(VERSION);
        
    }

}
