package com.bigdata.btree;

import com.bigdata.btree.IDataSerializer.DefaultDataSerializer;
import com.bigdata.btree.IDataSerializer.SimplePrefixSerializer;
import com.bigdata.btree.IIndexProcedure.IKeyArrayIndexProcedure;
import com.bigdata.service.IDataService;

/**
 * A factory for {@link IKeyArrayIndexProcedure}s so that their data may be key
 * range partitions and mapped against each relevant index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractIndexProcedureConstructor<T extends IKeyArrayIndexProcedure> {

    /**
     * Uses the {@link IDataSerializer}s reported by
     * {@link IndexMetadata#getLeafKeySerializer()} and
     * {@link IndexMetadata#getLeafValueSerializer()} for the specified
     * {@link IIndex}.
     * 
     * @param ndx
     *            The index - this is used to determine the serializers for the
     *            keys and/or values to be sent to a remote {@link IDataService}.
     * @param fromIndex
     *            The index of the first key to be used (inclusive).
     * @param toIndex
     *            The index of the last key to be used (exclusive).
     * @param keys
     *            The keys.
     * @param vals
     *            The values.
     * 
     * @return An instance of the procedure.
     * 
     * @todo we will need a different method signature to support
     *       hash-partitioned (vs range partitioned) indices.
     */
    public T newInstance(IIndex ndx, int fromIndex, int toIndex, byte[][] keys,
            byte[][] vals) {

        return newInstance(ndx.getIndexMetadata(), fromIndex, toIndex, keys, vals);
        
    }

    /**
     * Uses the {@link IDataSerializer}s reported by
     * {@link IndexMetadata#getLeafKeySerializer()} and
     * {@link IndexMetadata#getLeafValueSerializer()}.
     * 
     * @param indexMetadata
     * @param fromIndex
     * @param toIndex
     * @param keys
     * @param vals
     * @return
     */
    public T newInstance(IndexMetadata indexMetadata, int fromIndex, int toIndex,
            byte[][] keys, byte[][] vals) {
        
        return newInstance(indexMetadata.getLeafKeySerializer(),
                indexMetadata.getLeafValueSerializer(), fromIndex, toIndex, keys, vals);
        
    }

    /**
     * Uses default {@link IDataSerializer}s.
     * 
     * @param fromIndex
     * @param toIndex
     * @param keys
     * @param vals
     * @return
     */
    public T newInstance(int fromIndex, int toIndex, byte[][] keys,
            byte[][] vals) {

        return newInstance(SimplePrefixSerializer.INSTANCE,
                DefaultDataSerializer.INSTANCE, fromIndex, toIndex, keys, vals);
        
    }
       
    /**
     * Uses the specified {@link IDataSerializer}s.
     * 
     * @param keySer
     * @param valSer
     * @param fromIndex
     * @param toIndex
     * @param keys
     * @param vals
     * @return
     */
    abstract public T newInstance(IDataSerializer keySer, IDataSerializer valSer, int fromIndex, int toIndex,
            byte[][] keys, byte[][] vals);
        
// /**
// * Factory for object that will combine the results from each index
// * partition against which the procedure was mapped.
//         */
//        public H newResultHandler();
        
}
