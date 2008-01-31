/*

 Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jan 7, 2008
 */

package com.bigdata.rdf.store;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IParallelizableIndexProcedure;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure;
import com.bigdata.rdf.inf.Justification;

/**
 * Procedure for writing {@link Justification}s on an index or index
 * partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class WriteJustificationsProc
        extends AbstractKeyArrayIndexProcedure
        implements IParallelizableIndexProcedure {

    /**
     * 
     */
    private static final long serialVersionUID = -7469842097766417950L;

    /**
     * De-serialization constructor.
     *
     */
    public WriteJustificationsProc() {
        
        super();
        
    }
    
    public WriteJustificationsProc(int n, int offset, byte[][] keys) {
        
        super(n, offset, keys, null/* vals */);
        
    }
    
    /**
     * @return The #of justifications actually written on the index as a
     *         {@link Long}.
     */
    public Object apply(IIndex ndx) {

        long nwritten = 0;
        
        int n = getKeyCount();
        
        for (int i=0; i<n; i++) {

            byte[] key = getKey( i );

            if (!ndx.contains(key)) {

                ndx.insert(key, null/* no value */);

                nwritten++;

            }

        }
        
        return Long.valueOf(nwritten);
        
    }
    
    /**
     * @todo This method is not used. It could be implemented to change the data
     *       type for the operation to something that was more efficiently
     *       serialized than {@link Long}. It would have to be a mutable value
     *       as well.
     */
    final protected Object newResult() {
        
        throw new UnsupportedOperationException();
        
    }
    
}
