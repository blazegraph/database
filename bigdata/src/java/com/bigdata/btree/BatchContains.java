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

package com.bigdata.btree;

/**
 * Batch existence test operation. Existence tests SHOULD be used in place of
 * lookup tests to determine key existence if null values are allowed in an
 * index (lookup will return a null for both a null value and the absence of a
 * key in the index).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BatchContains extends AbstractKeyArrayIndexProcedure implements IBatchOperation,
        IReadOnlyOperation, IParallelizableIndexProcedure {

    /**
     * 
     */
    private static final long serialVersionUID = -5195874136364040815L;

    /**
     * Factory for {@link BatchContains} procedures.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class BatchContainsConstructor extends AbstractIndexProcedureConstructor<BatchContains> {

        public static final BatchContainsConstructor INSTANCE = new BatchContainsConstructor(); 
        
        private BatchContainsConstructor() {
            
        }
 
        public BatchContains newInstance(IDataSerializer keySer,
                IDataSerializer valSer, int fromIndex, int toIndex,
                byte[][] keys, byte[][] vals) {

            assert vals == null;
            
            return new BatchContains(keySer, fromIndex, toIndex, keys);
            
        }
        
    }

    /**
     * De-serialization ctor.
     *
     */
    public BatchContains() {
        
    }
    
    /**
     * Create a batch existence test operation.
     * 
     * @param keys
     *            A series of keys. Each key is an variable length unsigned
     *            byte[]. The keys MUST be presented in sorted order.
     * 
     * @see BatchContainsConstructor
     */
    protected BatchContains(IDataSerializer keySer, int fromIndex, int toIndex,
            byte[][] keys) {

        super(keySer, null, fromIndex, toIndex, keys, null/*vals*/);

    }

    /**
     * Applies the operation using {@link ISimpleBTree#contains(byte[])}.
     * 
     * @param ndx
     * 
     * @return A {@link ResultBitBuffer}.
     */
    public Object apply(IIndex ndx) {

        final int n = getKeyCount();
        
        final boolean[] ret = new boolean[n];
        
        int i = 0;
        
        while( i < n ) {

            ret[i] = ndx.contains(getKey(i));

            i++;

        }

        return new ResultBitBuffer(n,ret);
        
    }
    
}
