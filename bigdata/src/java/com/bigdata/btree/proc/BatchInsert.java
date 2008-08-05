/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
import com.bigdata.btree.IDataSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ISimpleBTree;

/**
 * Batch insert operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BatchInsert extends AbstractKeyArrayIndexProcedure implements
        IParallelizableIndexProcedure {

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

    public final boolean isReadOnly() {
        
        return false;
        
    }
    
    /**
     * Factory for {@link BatchInsert} procedures.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class BatchInsertConstructor extends AbstractIndexProcedureConstructor<BatchInsert> {

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
        
        private BatchInsertConstructor(boolean returnOldValues) {
            
            this.returnOldValues = returnOldValues;
            
        }
        
        public BatchInsert newInstance(IDataSerializer keySer,
                IDataSerializer valSer, int fromIndex, int toIndex,
                byte[][] keys, byte[][] vals) {

            return new BatchInsert(keySer,valSer,fromIndex, toIndex, keys, vals, returnOldValues);
            
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
    protected BatchInsert(IDataSerializer keySer, IDataSerializer valSer,
            int fromIndex, int toIndex, byte[][] keys, byte[][] vals,
            boolean returnOldValues) {

        super(keySer, valSer, fromIndex, toIndex, keys, vals);

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
    public ResultBuffer apply(IIndex ndx) {

        int i = 0;
        
        final int n = getKeyCount();

        final byte[][] ret = (returnOldValues ? new byte[n][] : null);
        
//        try {
        
        while (i < n) {

            final byte[] key = getKey(i);
            
            final byte[] val = getValue(i);
            
            final byte[] old = (byte[]) ndx.insert(key,val);
            
            if(returnOldValues) {
                
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
                    .getTupleSerializer().getLeafValueSerializer());
            
        }
        
        return null;

    }
    
    @Override
    protected void readMetadata(ObjectInput in) throws IOException, ClassNotFoundException {

        super.readMetadata(in);

        returnOldValues = in.readBoolean();

    }

    @Override
    protected void writeMetadata(ObjectOutput out) throws IOException {

        super.writeMetadata(out);

        out.writeBoolean(returnOldValues);

    }

}
