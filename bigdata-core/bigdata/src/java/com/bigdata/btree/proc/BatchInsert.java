/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Feb 12, 2007
 */

package com.bigdata.btree.proc;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.btree.Errors;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ISimpleBTree;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.service.ndx.NopAggregator;

/**
 * Batch insert operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BatchInsert extends AbstractKeyArrayIndexProcedure<ResultBuffer> implements
        IParallelizableIndexProcedure<ResultBuffer> {

    /**
     * 
     */
    private static final long serialVersionUID = 6594362044816120035L;

    private boolean returnOldValues;
    
    /**
     * True iff the old values stored under the keys will be returned by
     * {@link #apply(IIndex)}.
     */
    public boolean getReturnOldValues() {

        return returnOldValues;
        
    }

    @Override
    public final boolean isReadOnly() {
        
        return false;
        
    }
    
    /**
     * Factory for {@link BatchInsert} procedures.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static class BatchInsertConstructor extends
            AbstractKeyArrayIndexProcedureConstructor<BatchInsert> {

        /**
         * Singleton requests the return of the old values that were overwritten
         * in the index by the operation.
         */
        public static final BatchInsertConstructor RETURN_OLD_VALUES = new BatchInsertConstructor(true);
        
        /**
         * Singleton does NOT request the return of the old values that were
         * overwritten in the index by the operation.
         */
        public static final BatchInsertConstructor RETURN_NO_VALUES = new BatchInsertConstructor(false); 

        private boolean returnOldValues;
        
        private BatchInsertConstructor(final boolean returnOldValues) {

            this.returnOldValues = returnOldValues;

        }

        /**
         * Values are required.
         */
        @Override
        public final boolean sendValues() {

            return true;

        }

        @Override
        public BatchInsert newInstance(final IRabaCoder keysCoder,
            	final IRabaCoder valsCoder, final int fromIndex, final int toIndex,
                final byte[][] keys, final byte[][] vals) {

            return new BatchInsert(keysCoder, valsCoder, fromIndex, toIndex,
                    keys, vals, returnOldValues);

        }
        
    }
    
    /**
     * De-serialization ctor.
     *
     */
    public BatchInsert() {
        
    }
    
    /**
     * Create a batch insert operation.
     * <p>
     * Batch insert operation of N tuples presented in sorted order. This
     * operation can be very efficient if the tuples are presented sorted by key
     * order.
     * 
     * @param keys
     *            A series of keys paired to values. Each key is an variable
     *            length unsigned byte[]. The keys MUST be presented in sorted
     *            order.
     * @param vals
     *            An array of values corresponding to those keys. Null elements
     *            are allowed.
     * @param returnOldValues
     *            When <code>true</code> the old values for those keys will be
     *            returned by {@link #apply(IIndex)}.
     * 
     * @see BatchInsertConstructor
     */
    protected BatchInsert(final IRabaCoder keysCoder, final IRabaCoder valsCoder,
            final int fromIndex, final int toIndex, final byte[][] keys, final byte[][] vals,
            final boolean returnOldValues) {

        super(keysCoder, valsCoder, fromIndex, toIndex, keys, vals);

        if (vals == null)
            throw new IllegalArgumentException(Errors.ERR_VALS_NULL);

        this.returnOldValues = returnOldValues;

    }
    
    /**
     * Applies the operator using {@link ISimpleBTree#insert(Object, Object)}
     * 
     * @param ndx
     * 
     * @return Either <code>null</code> if the old values were not requested
     *         or a {@link ResultBuffer} containing the old values.
     */
    @Override
    public ResultBuffer applyOnce(final IIndex ndx, final IRaba keys, final IRaba vals) {

        int i = 0;
        
        final int n = keys.size();

        final byte[][] ret = (returnOldValues ? new byte[n][] : null);
        
//        try {
        
        while (i < n) {

            final byte[] key = keys.get(i);
            
            final byte[] val = vals.get(i);

            final byte[] old = (byte[]) ndx.insert(key, val);

            if (returnOldValues) {
                
                ret[i] = old;
                
            }
            
            i++;
            
        }
        
//        } catch(RuntimeException ex) {
//        
//            // remove this debugging try...catch code.
//            if(DEBUG && ex.getMessage().contains("KeyAfterPartition")) {
//                
//                log.debug("keys: "+toString(keys));
//                
////                System.exit(1);
//                
//            }
//            
//            throw ex;
//            
//        }
        
        if (returnOldValues) {
            
            return new ResultBuffer(n, ret, ndx.getIndexMetadata()
                    .getTupleSerializer().getLeafValuesCoder());
            
        }
        
        return null;

    }
    
    @Override
    protected void readMetadata(final ObjectInput in) throws IOException, ClassNotFoundException {

        super.readMetadata(in);

        returnOldValues = in.readBoolean();

    }

    @Override
    protected void writeMetadata(final ObjectOutput out) throws IOException {

        super.writeMetadata(out);

        out.writeBoolean(returnOldValues);

    }

	@SuppressWarnings("unchecked")
	@Override
	protected IResultHandler<ResultBuffer, ResultBuffer> newAggregator() {

		if (!getReturnOldValues()) {

			// NOP aggegrator preserves striping against the index.
			return NopAggregator.INSTANCE;

		}
		
		return new ResultBufferHandler(getKeys().size(), getValuesCoder());

	}

}
