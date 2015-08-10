package com.bigdata.btree;

import java.util.Arrays;

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.rawstore.IBlock;

/**
 * Test helper for a tuple with static data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
public class TestTuple<E> implements ITuple<E> {

    private final int flags;

    private final byte[] key;

    private final byte[] val;

    private final boolean deleted;

    final private long timestamp;

    private final ITupleSerializer tupleSer;
    
    public TestTuple(Object key, E value) {

        this(IRangeQuery.DEFAULT, key, value);

    }

    public TestTuple(int flags, Object key, E value) {

        this(flags, key, value, false/* deleted */, 0L/* timestamp */);

    }

    public TestTuple(int flags, Object key, E val, boolean deleted,
            long timestamp) {

        this(flags, DefaultTupleSerializer.newInstance(), key, val, deleted,
                timestamp);
        
    }
    
    public TestTuple(int flags, ITupleSerializer<Object, E> tupleSer,
            Object key, E val, boolean deleted, long timestamp) {

        this.flags = flags;

        this.tupleSer = tupleSer;
        
        this.key = tupleSer.serializeKey(key);

        this.val = tupleSer.serializeVal(val);

        this.deleted = deleted;

        this.timestamp = timestamp;

    }

    public TestTuple(int flags, ITupleSerializer<Object, E> tupleSer,
            byte[] key, byte[] val, boolean deleted, long timestamp) {

        this.flags = flags;

        this.tupleSer = tupleSer;
        
        this.key = key;

        this.val = val;

        this.deleted = deleted;

        this.timestamp = timestamp;

    }

    public String toString() {
        
        return super.toString()+
        "{flags="+AbstractTuple.flagString(flags)+
        (isDeletedVersion()? ", deleted" : "")+
        (getVersionTimestamp() == 0L ? "" : ", timestamp="+ getVersionTimestamp())+
        ", key="+(getKeysRequested()?Arrays.toString(getKey()):"N/A")+
        ", val="+(getValuesRequested()?(isNull()?"null":Arrays.toString(getValue())):"N/A")+
        ", obj="+getObject()+
        "}";
        
    }
    
    public int flags() {

        return flags;

    }

    public byte[] getKey() {

        return key;

    }

    public ByteArrayBuffer getKeyBuffer() {

        return new ByteArrayBuffer(0, key.length, key);

    }

    public DataInputBuffer getKeyStream() {

        return new DataInputBuffer(key);

    }

    public boolean getKeysRequested() {

        return ((flags & IRangeQuery.KEYS) != 0);

    }

    @SuppressWarnings("unchecked")
    public E getObject() {

        return (E) tupleSer.deserialize(this);

    }

    public int getSourceIndex() {
        // TODO Auto-generated method stub
        return 0;
    }

    public byte[] getValue() {
        
        return val;
        
    }

    public ByteArrayBuffer getValueBuffer() {

        if (val == null)
            throw new UnsupportedOperationException();

        return new ByteArrayBuffer(0, val.length, val);

    }

    public DataInputBuffer getValueStream() {

        return new DataInputBuffer(val);

    }

    public boolean getValuesRequested() {

        return ((flags & IRangeQuery.VALS) != 0);

    }

    public long getVersionTimestamp() {
        return timestamp;
    }

    public long getVisitCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    public boolean isDeletedVersion() {
        return deleted;
    }

    public boolean isNull() {
        return val == null;
    }

    public IBlock readBlock(long addr) {
        // TODO Auto-generated method stub
        return null;
    }

    public ITupleSerializer getTupleSerializer() {

        return tupleSer;
        
    }

}
