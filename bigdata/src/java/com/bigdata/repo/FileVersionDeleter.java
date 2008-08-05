package com.bigdata.repo;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.filter.Advancer;
import com.bigdata.btree.filter.TupleUpdater;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.sparse.KeyDecoder;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.sparse.TimestampChooser;
import com.bigdata.sparse.ValueType;
import com.bigdata.sparse.TPS.TPV;

/**
 * A procedure that performs a key range scan, marking all non-deleted
 * versions within the key range as deleted (by storing a null property
 * value for the {@link MetadataSchema#VERSION}).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FileVersionDeleter extends TupleUpdater<TPV> {

    private static final long serialVersionUID = -3084247827028423921L;

    /**
     * The timestamp specified to the ctor.
     */
    private final long timestamp;
    
    /**
     * The timestamp choosen on the server (not serialized).
     */
    transient private long choosenTimestamp;
    
    /** <code>false</code> until we choose the timestamp (not serialized) */
    transient private boolean didInit = false;

    /** used to generate keys on the server (not serialized). */
    transient private KeyBuilder keyBuilder;
    
    /**
     * 
     * @param timestamp
     *            A valid timestamp or {@link SparseRowStore#AUTO_TIMESTAMP}
     *            or {@link SparseRowStore#AUTO_TIMESTAMP_UNIQUE}.
     */
    public FileVersionDeleter(long timestamp) {
        
        this.timestamp = timestamp;
        
    }
    
    /**
     * Only visits the {@link MetadataSchema#VERSION} columns.
     */
    @Override
    protected boolean isValid(ITuple<TPV> tuple) {

        KeyDecoder keyDecoder = new KeyDecoder(tuple.getKey());
        
        String name = keyDecoder.getColumnName();
        
        if(!name.equals(MetadataSchema.VERSION)) return false;

        return true;
        
    }

    /**
     * Appends a new tuple into the index whose key uses the
     * {@link #choosenTimestamp} and whose value is an encoded <code>null</code>.
     * This is interepreted as a "deleted" file version tuple.
     * <p>
     * Note: The old tuple is not removed so you can continue to access the
     * historical version explicitly.
     * 
     * @todo unit tests to verify that old versions are left behind on overflow
     *       (per the appropriate policy).
     * 
     * @todo unit test to verify continued access to the historical version.
     * 
     * FIXME Unit test to verify that the iterator correctly visits all file
     * versions in the range. In particular, we will be seeing each file version
     * tuple in the key range. There may be more than one of these, right?
     * Therefore we need to use the {@link Advancer} pattern to skip beyond the
     * last possible such tuple (which will in fact be the one that we write on
     * here!).
     * <p>
     * One way to approach this is a logical row scan with a column name filter
     * that only visits the "version" column and an {@link TupleUpdater} and writes a
     * new tuple per the code below. We just need to make sure that we either do
     * not then visit the new tuple or that we ignore a "version" row whose
     * value is [null].
     */
    protected void update(IIndex ndx, ITuple<TPV> tuple) {
                    
        final byte[] key = tuple.getKey();
        
        if (!didInit) {

            // choose timestamp for any writes.
            choosenTimestamp = TimestampChooser.chooseTimestamp(ndx,
                    timestamp);
            
            keyBuilder = new KeyBuilder(key.length);
            
            didInit = true;
            
        }

//        // remove the old tuple.
//        ndx.remove(key);
        
        // copy everything from the key except the old timestamp.
        keyBuilder.reset().append(0/* off */,
                key.length - Bytes.SIZEOF_LONG, key);
        
        // append the new timestamp.
        keyBuilder.append(choosenTimestamp);
        
        /*
         * insert a new tuple using the new timestamp. the [null] will be
         * interpreted as a deleted file version.
         */
        ndx.insert(key, ValueType.encode(null));
        
    }

}
