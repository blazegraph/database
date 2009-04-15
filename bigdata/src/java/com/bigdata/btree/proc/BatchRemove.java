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

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.compression.IDataSerializer;

/**
 * Batch removal of one or more tuples, optionally returning their existing
 * values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BatchRemove extends AbstractKeyArrayIndexProcedure implements
        IParallelizableIndexProcedure {

    /**
     * 
     */
    private static final long serialVersionUID = -5332443478547654844L;

    private boolean assertFound;
    private boolean returnOldValues;

    /**
     * True iff the procedure will verify that each supplied key was in fact
     * found in the index.
     */
    public boolean getAssertFound() {

        return assertFound;

    }

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
     * Factory for {@link BatchRemove} procedures.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class BatchRemoveConstructor extends
            AbstractKeyArrayIndexProcedureConstructor<BatchRemove> {

        /**
         * Singleton requests the return of the values that were removed from
         * the index by the operation.
         */
        public static final BatchRemoveConstructor RETURN_OLD_VALUES = new BatchRemoveConstructor(
                false, true);

        /**
         * Singleton does NOT request the return of the values that were removed
         * from the index by the operation.
         */
        public static final BatchRemoveConstructor RETURN_NO_VALUES = new BatchRemoveConstructor(
                false, false);

        /**
         * Singleton does NOT request the return of the values that were removed
         * from the index by the operation but asserts that each key was in fact
         * present in the index.
         */
        public static final BatchRemoveConstructor ASSERT_FOUND_RETURN_NO_VALUES = new BatchRemoveConstructor(
                true, false);

        private final boolean assertFound;
        private final boolean returnOldValues;

        /**
         * Values ARE NOT sent.
         */
        public final boolean sendValues() {
            
            return false;
            
        }

        private BatchRemoveConstructor(boolean assertFound, boolean returnOldValues) {

            this.assertFound = assertFound;
            
            this.returnOldValues = returnOldValues;

        }

        public BatchRemove newInstance(IDataSerializer keySer,
                IDataSerializer valSer, int fromIndex, int toIndex,
                byte[][] keys, byte[][] vals) {

            if(vals != null) throw new IllegalArgumentException("vals should be null");

            return new BatchRemove(keySer,valSer,fromIndex, toIndex, keys, assertFound, returnOldValues);

        }

    }

    /**
     * De-serialization ctor.
     * 
     */
    public BatchRemove() {

    }

    /**
     * Batch remove operation.
     * 
     * @param keys
     *            A series of keys paired to values. Each key is an variable
     *            length unsigned byte[]. The keys MUST be presented in sorted
     *            order.
     * @param returnOldValues
     *            When <code>true</code> the old values for those keys will be
     *            returned by {@link #apply(IIndex)}.
     * 
     * @see BatchRemoveConstructor
     */
    protected BatchRemove(IDataSerializer keySer, IDataSerializer valSer,
            int fromIndex, int toIndex, byte[][] keys, boolean assertFound,
            boolean returnOldValues) {

        super(keySer, valSer, fromIndex, toIndex, keys, null/* vals */);

        this.assertFound = assertFound;

        this.returnOldValues = returnOldValues;

    }

    /**
     * Applies the operation.
     * 
     * @param ndx
     * 
     * @return The old values as a {@link ResultBuffer} iff they were requested.
     * 
     * @throws AssertionError
     *             if {@link #getAssertFound()} is <code>true</code> and a
     *             given key was not found in the index.
     */
    public Object apply(IIndex ndx) {

        final int n = getKeyCount();

        final byte[][] ret = returnOldValues ? new byte[n][] : null;

        int i = 0;

        while (i < n) {

            final byte[] key = getKey(i);
            
            final byte[] oldval = ndx.remove(key);

            if(assertFound) {
                
                if (oldval == null) {

                    throw new AssertionError("No entry: "
                            + BytesUtil.toString(key));
                    
                }
                
            }
            
            if (returnOldValues) {

                ret[i] = oldval;

            }

            i++;

        }

        if (returnOldValues) {

            return new ResultBuffer(n, ret, ndx.getIndexMetadata()
                    .getTupleSerializer().getLeafValueSerializer());

        }

        return null;

    }

    @Override
    protected void readMetadata(ObjectInput in) throws IOException, ClassNotFoundException {

        super.readMetadata(in);

        assertFound = in.readBoolean();
        
        returnOldValues = in.readBoolean();

    }

    @Override
    protected void writeMetadata(ObjectOutput out) throws IOException {

        super.writeMetadata(out);
        
        out.writeBoolean(assertFound);

        out.writeBoolean(returnOldValues);

    }

}
