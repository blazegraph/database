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
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;

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
    private ReturnWhatEnum returnWhat;

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

        return returnWhat == ReturnWhatEnum.OldValues;

    }

    public final boolean isReadOnly() {
        
        return false;
        
    }

    /**
     * What to return.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private static enum ReturnWhatEnum {
      
        /**
         * Return the #of tuples that were deleted.
         */
        MutationCount(0),
        
        /**
         * Return the old value for each tuple.
         */
        OldValues(1),
        
        /**
         * Return a {@link ResultBitBuffer}, which is basically a bit mask
         * indicating which of the caller's tuples were deleted.
         */
        BitMask(2);
        
        private final int w;
        
        private ReturnWhatEnum(int w) {
            this.w = w;
        }

        public int getValue() {
            return w;
        }
        
        public static ReturnWhatEnum valueOf(final int w) {
            switch (w) {
            case 0:
                return MutationCount;
            case 1:
                return OldValues;
            case 2:
                return BitMask;
            default:
                throw new IllegalArgumentException();
            }
        }
        
    };
    
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
                false, ReturnWhatEnum.OldValues);

        /**
         * Singleton does NOT request the return of the values that were removed
         * from the index by the operation. Instead, only the #of deleted tuples
         * is return (the mutationCount).
         */
        public static final BatchRemoveConstructor RETURN_MUTATION_COUNT = new BatchRemoveConstructor(
                false, ReturnWhatEnum.MutationCount);

        /**
         * Singleton requests the return of a {@link ResultBitBuffer} providing
         * a bit mask of the tuples which were removed from the index by this
         * operation (that is, those tuples which were pre-existing in the index
         * in a non-deleted state).
         */
        public static final BatchRemoveConstructor RETURN_BIT_MASK = new BatchRemoveConstructor(
                false, ReturnWhatEnum.BitMask);

        /**
         * Singleton does NOT request the return of the values that were removed
         * from the index by the operation but asserts that each key was in fact
         * present in the index.
         */
        public static final BatchRemoveConstructor ASSERT_FOUND_RETURN_NO_VALUES = new BatchRemoveConstructor(
                true, ReturnWhatEnum.BitMask);

        private final boolean assertFound;
        private final ReturnWhatEnum returnWhat;

        /**
         * Values ARE NOT sent.
         */
        public final boolean sendValues() {
            
            return false;
            
        }

        private BatchRemoveConstructor(final boolean assertFound,
                final ReturnWhatEnum returnWhat) {

            this.assertFound = assertFound;
            
            this.returnWhat = returnWhat;

        }

        public BatchRemove newInstance(IRabaCoder keySer, IRabaCoder valSer,
                int fromIndex, int toIndex, byte[][] keys, byte[][] vals) {

            if (vals != null)
                throw new IllegalArgumentException("vals should be null");

            return new BatchRemove(keySer, valSer, fromIndex, toIndex, keys,
                    assertFound, returnWhat);

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
    protected BatchRemove(IRabaCoder keySer, IRabaCoder valSer,
            int fromIndex, int toIndex, byte[][] keys, boolean assertFound,
            ReturnWhatEnum returnWhat) {

        super(keySer, valSer, fromIndex, toIndex, keys, null/* vals */);

        this.assertFound = assertFound;

        this.returnWhat = returnWhat;

        if (returnWhat == null)
            throw new IllegalArgumentException();
        
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
    public Object apply(final IIndex ndx) {

        final int n = getKeyCount();

        final boolean returnOldValues = getReturnOldValues();
        
        final byte[][] ret = returnOldValues ? new byte[n][] : null;

        final boolean[] modified = returnWhat == ReturnWhatEnum.BitMask ? new boolean[n]
                : null;
        
        int i = 0, mutationCount = 0;

        final IRaba keys = getKeys();
        
        while (i < n) {

            final byte[] key = keys.get(i);
            
            if (!returnOldValues && ndx.contains(key)) {

                // Track a mutation counter.
                mutationCount++;
                
                if (modified != null) {

                    modified[i] = true;

                }
                
            }

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

        switch (returnWhat) {
        
        case MutationCount:
            
            return Long.valueOf(mutationCount);
        
        case OldValues:
        
            return new ResultBuffer(n, ret, ndx.getIndexMetadata()
                    .getTupleSerializer().getLeafValuesCoder());
        
        case BitMask:
            
            return new ResultBitBuffer(n, modified, mutationCount);
        
        default:
            throw new AssertionError();
        
        }
        
    }

    @Override
    protected void readMetadata(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        super.readMetadata(in);

        assertFound = in.readBoolean();

        returnWhat = ReturnWhatEnum.valueOf(in.readByte());

    }

    @Override
    protected void writeMetadata(final ObjectOutput out) throws IOException {

        super.writeMetadata(out);

        out.writeBoolean(assertFound);

        out.writeByte((byte) returnWhat.getValue());

    }

}
