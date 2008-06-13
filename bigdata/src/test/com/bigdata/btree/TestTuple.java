package com.bigdata.btree;

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.IBlock;

/**
 * Test helper for a tuple with static data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
class TestTuple<E> implements ITuple<E> {

    private final int flags;

    private final byte[] key;

    private final byte[] val;

    private final boolean deleted;

    final private long timestamp;

    public TestTuple(Object key, E value) {

        this(IRangeQuery.DEFAULT, key, value);

    }

    public TestTuple(int flags, Object key, E value) {

        this(flags, key, value, false/* deleted */, 0L/* timestamp */);

    }

    public TestTuple(int flags, Object key, E val, boolean deleted,
            long timestamp) {

        this.flags = flags;

        this.key = KeyBuilder.asSortKey(key);

        this.val = SerializerUtil.serialize(val);

        this.deleted = deleted;

        this.timestamp = timestamp;

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

        return (E) SerializerUtil.deserialize(val);

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

        return DefaultTupleSerializer.INSTANCE;
        
    }

}