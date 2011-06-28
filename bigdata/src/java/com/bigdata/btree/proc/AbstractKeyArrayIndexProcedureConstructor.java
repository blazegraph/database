package com.bigdata.btree.proc;

import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.service.IDataService;

/**
 * A factory for {@link IKeyArrayIndexProcedure}s so that their data may be key
 * range partitions and mapped against each relevant index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractKeyArrayIndexProcedureConstructor<T extends IKeyArrayIndexProcedure> {

    /**
     * Return <code>true</code> if the procedure requires values paired with
     * the keys (otherwise the caller should specify <code>null</code> for the
     * values byte[]).
     */
    abstract public boolean sendValues();
    
    /**
     * Uses the {@link ITupleSerializer} reported by {@link IndexMetadata} for
     * the {@link IIndex}.
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
     *            The values (may be optional depending on the semantics of the
     *            operation).
     * 
     * @return An instance of the procedure.
     * 
     * @todo we will need a different method signature to support
     *       hash-partitioned (vs range partitioned) indices.
     */
    public T newInstance(final IIndex ndx, final int fromIndex,
            final int toIndex, final byte[][] keys, final byte[][] vals) {

        return newInstance(ndx.getIndexMetadata(), fromIndex, toIndex, keys, vals);
        
    }

    /**
     * Uses the {@link ITupleSerializer} reported by {@link IndexMetadata}.
     * 
     * @param indexMetadata
     * @param fromIndex
     * @param toIndex
     * @param keys
     * @param vals
     * @return
     */
    public T newInstance(final IndexMetadata indexMetadata,
            final int fromIndex, final int toIndex, final byte[][] keys,
            final byte[][] vals) {

        final ITupleSerializer tupleSer = indexMetadata.getTupleSerializer();

        return newInstance(tupleSer.getLeafKeysCoder(), tupleSer
                .getLeafValuesCoder(), fromIndex, toIndex, keys, vals);

    }

    /**
     * Uses the default {@link IRabaCoder}s for coding.
     * 
     * @param fromIndex
     * @param toIndex
     * @param keys
     * @param vals
     * @return
     * 
     * @todo Why does this variant exist? Is it just to make life easier when
     *       the {@link IndexMetadata} is not on hand locally?
     */
    public T newInstance(final int fromIndex, final int toIndex,
            final byte[][] keys, final byte[][] vals) {

        return newInstance(
                DefaultTupleSerializer.getDefaultLeafKeysCoder(),
                DefaultTupleSerializer.getDefaultValuesCoder(),
                fromIndex, toIndex, keys, vals);
        
    }
       
    /**
     * Uses the specified {@link IRabaCoder}s.
     * 
     * @param keysCoder
     * @param valsCoder
     * @param fromIndex
     * @param toIndex
     * @param keys
     * @param vals
     * @return
     */
    abstract public T newInstance(IRabaCoder keysCoder,
            IRabaCoder valsCoder, int fromIndex, int toIndex, byte[][] keys,
            byte[][] vals);
        
}
