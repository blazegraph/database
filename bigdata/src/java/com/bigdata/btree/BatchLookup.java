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
 * Batch lookup operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BatchLookup extends IndexProcedure implements IBatchOperation, IReadOnlyOperation, IParallelizableIndexProcedure {

    /**
     * 
     */
    private static final long serialVersionUID = 8102720738892338403L;

    /**
     * Factory for {@link BatchLookup} procedures.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class BatchLookupConstructor implements IIndexProcedureConstructor {

        public static final BatchLookupConstructor INSTANCE = new BatchLookupConstructor(); 
        
        public BatchLookupConstructor() {
            
        }
        
        public IIndexProcedure newInstance(int n, int offset, byte[][] keys, byte[][] vals) {

            return new BatchLookup(n, offset, keys);
            
        }
        
    }
    
    /**
     * De-serialization ctor.
     *
     */
    public BatchLookup() {
        
    }
    
    /**
     * Create a batch lookup operation.
     * 
     * @param ntuples
     *            The #of tuples in the operation (in).
     * @param offset
     *            The offset into <i>keys</i> and <i>vals</i> of the 1st
     *            tuple.
     * @param keys
     *            The array of keys (one key per tuple).
     */
    public BatchLookup(int ntuples, int offset, byte[][] keys) {

        super(ntuples, offset, keys, null/* values */);
        
    }

    /**
     * @return {@link ResultBuffer}
     */
    public Object apply(IIndex ndx) {

        final int n = getKeyCount();
        
        final byte[][] ret = new byte[n][];
        
        int i = 0;
        
        while (i < n) {

            ret[i] = (byte[]) ndx.lookup(getKey(i));

            i ++;

        }

        return new ResultBuffer(n, ret, getResultSerializer());
        
    }
    
}
